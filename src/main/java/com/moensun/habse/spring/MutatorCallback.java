package com.moensun.habse.spring;

import org.apache.hadoop.hbase.client.BufferedMutator;

/**
 * BufferedMutator通过mutate方法提交数据
 */
public interface MutatorCallback {
    void doInMutator(BufferedMutator mutator) throws Throwable;
}
