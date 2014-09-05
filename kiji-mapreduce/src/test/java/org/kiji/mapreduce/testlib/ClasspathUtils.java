/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.mapreduce.testlib;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import org.kiji.mapreduce.DistributedCacheJars;

public final class ClasspathUtils {
  public static final int UNIQUE_DIR_ATTEMPTS = 1000;

  /** Static components to help build a JAR file. */
  public static final String JAR_EXTENSION = ".jar";
  public static final Manifest JAR_MANIFEST;
  static {
    JAR_MANIFEST = new Manifest();
    JAR_MANIFEST.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
  }

  /**
   * Disable constructor for utility class.
   */
  private ClasspathUtils() { }

  /**
   * Returns the classpath as a list of paths.
   *
   * @return the classpath as a list of paths.
   * @throws java.io.IOException if there is an error writing a jar file.
   */
  public static List<Path> getCurrentClasspathEntries() throws IOException {
    final String classpath = System.getProperty("java.class.path");
    final File jarDir = createTempDir("jars-from-classpath", "");

    final ImmutableList.Builder<Path> classpathBuilder = ImmutableList.builder();
    for (final String entry : classpath.split(File.pathSeparator)) {
      final File entryFile = new File(entry).getAbsoluteFile();

      if (entryFile.isDirectory()) {
        final File jarFile = File.createTempFile(
            entryFile.getName(),
            JAR_EXTENSION,
            jarDir
        );
        makeJarFromDirectory(entryFile, jarFile);
        classpathBuilder.add(new Path(jarFile.toURI()));
      } else {
        classpathBuilder.add(new Path(entryFile.toURI()));
      }
    }
    return classpathBuilder.build();
  }

  /**
   * Given a path to a directory, construct a temporary jar with its contents.
   *
   * @param directory to build the jar from.
   * @param jarLocation to write the jar to.
   * @throws java.io.IOException if jar file construction fails.
   */
  public static void makeJarFromDirectory(
      final File directory,
      final File jarLocation
  ) throws IOException {
    final FileOutputStream fileOutputStream = new FileOutputStream(jarLocation);
    try {
      final JarOutputStream jarOutputStream =
          new JarOutputStream(fileOutputStream, JAR_MANIFEST);
      try {
        // This is a recursive method. Have it start adding folders at the content root.
        jarFileAdd(directory, directory, jarOutputStream);
      } finally {
        jarOutputStream.close();
      }
    } finally {
      fileOutputStream.close();
    }
  }

  /**
   * Creates a temporary directory that works with YARN on mac and linux.
   *
   * This will create a temporary directory in target/
   *
   * @param prefix to prepend the temporary directory name with.
   * @param suffix to suffix the temporary directory name with.
   * @return a temporary directory that works with YARN on mac and linux.
   */
  public static File createTempDir(final String prefix, final String suffix) throws IOException {
    // We can't use the system temporary directory since on macs the permissions for this directory
    // by default will not work with the yarn resource localizer (it requires that all parent
    // directories of files have read and execute permissions for the "other" octal digit).
    return createUniqueDir(prefix, suffix, new File("target"));
  }

  /**
   * Creates a unique directory in the specified directory.
   *
   * @param prefix to prepend the temporary directory name with.
   * @param suffix to suffix the temporary directory name with.
   * @param directory to create the temporary directory in.
   * @return a temporary directory that works with YARN on mac and linux.
   */
  public static File createUniqueDir(
      final String prefix,
      final String suffix,
      final File directory
  ) throws IOException {
    // This was originally copied from Guava's Files#createTempDir().
    final String baseName;
    if (prefix.equals("")) {
      baseName = Long.toString(System.currentTimeMillis());
    } else {
      baseName = String.format("%s-%d", prefix, System.currentTimeMillis());
    }

    for (int counter = 0; counter < UNIQUE_DIR_ATTEMPTS; counter++) {
      final String tempDirName;
      if (suffix.equals("")) {
        tempDirName = String.format("%s-%d", baseName, counter);
      } else {
        tempDirName = String.format("%s-%d-%s", baseName, counter, suffix);
      }
      final File tempDir = new File(directory, tempDirName);
      if (tempDir.mkdirs()) {
        return tempDir;
      }
    }
    throw new IllegalStateException(
        String.format(
            "Failed to create directory in %s within %s attempts",
            directory,
            UNIQUE_DIR_ATTEMPTS
        )
    );
  }

  /**
   * Helper method to recursively add a directory to a jar.
   *
   * @see <a href=
   * "http://stackoverflow.com/questions/1281229/how-to-use-jaroutputstream-to-create-a-jar-file">
   * http://stackoverflow.com/questions/1281229/how-to-use-jaroutputstream-to-create-a-jar-file</a>
   *
   * @param root directory of the jar file contents.
   * @param source file or directory to add to the jar. Directory contents are recursively added as
   *     well.
   * @param jarOutputStream to write to.
   * @throws java.io.IOException if a file copy into jar fails.
   */
  public static void jarFileAdd(
      final File root,
      final File source,
      final JarOutputStream jarOutputStream
  ) throws IOException {
    BufferedInputStream in = null;
    try {
      if (source.isDirectory()) {
        String name = root.toURI().relativize(source.toURI()).getPath();
        if (!name.isEmpty()) {
          if (!name.endsWith("/")) {
            name += "/";
          }
          JarEntry entry = new JarEntry(name);
          entry.setTime(source.lastModified());
          jarOutputStream.putNextEntry(entry);
          jarOutputStream.closeEntry();
        }
        for (File nestedFile : source.listFiles()) {
          jarFileAdd(root, nestedFile, jarOutputStream);
        }
      } else {
        final JarEntry entry = new JarEntry(root.toURI().relativize(source.toURI()).getPath());
        entry.setTime(source.lastModified());
        jarOutputStream.putNextEntry(entry);
        in = new BufferedInputStream(new FileInputStream(source));
        final byte[] buffer = new byte[1024];
        while (true) {
          int count = in.read(buffer);
          if (count == -1) {
            break;
          }
          jarOutputStream.write(buffer, 0, count);
        }
        jarOutputStream.closeEntry();
      }
    } finally {
      if (in != null) {
        in.close();
      }
    }
  }
}
