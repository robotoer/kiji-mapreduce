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

package org.kiji.mapreduce.platform;

import java.io.IOException;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import org.kiji.annotations.ApiAudience;

/**
 * CDH4-backed implementation of the KijiMRPlatformBridge API.
 */
@ApiAudience.Private
public final class CDH4MR1KijiMRBridge extends KijiMRPlatformBridge {
  /** {@inheritDoc} */
  @Override
  public TaskAttemptContext newTaskAttemptContext(Configuration conf, TaskAttemptID id) {
    // In CDH4, TaskAttemptContext and its implementation are separated.
    return new TaskAttemptContextImpl(conf, id);
  }

  /** {@inheritDoc} */
  @Override
  public TaskAttemptID newTaskAttemptID(String jtIdentifier, int jobId, TaskType type,
      int taskId, int id) {
    // In CDH4, use all these args directly.
    return new TaskAttemptID(jtIdentifier, jobId, type, taskId, id);
  }

  /** {@inheritDoc} */
  @Override
  public SequenceFile.Writer newSeqFileWriter(Configuration conf, Path filename,
      Class<?> keyClass, Class<?> valueClass) throws IOException {

    Preconditions.checkArgument(conf != null, "Configuration argument must be non-null");
    Preconditions.checkArgument(filename != null, "Filename argument must be non-null");

    return SequenceFile.createWriter(conf,
        SequenceFile.Writer.file(filename),
        SequenceFile.Writer.keyClass(keyClass),
        SequenceFile.Writer.valueClass(valueClass));
  }

  /** {@inheritDoc} */
  @Override
  public void setUserClassesTakesPrecedence(Job job, boolean value) {
    // We can do this directly in CDH4.
    job.setUserClassesTakesPrecedence(value);
  }
}
