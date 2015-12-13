package com.bda.functions;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public abstract class NoSplitFileInputFormat extends FileInputFormat
{

    @Override
    protected boolean isSplitable(JobContext context, Path file)
    {
        return false;
    }
}
