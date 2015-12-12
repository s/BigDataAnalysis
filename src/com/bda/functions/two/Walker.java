package com.bda.functions.two;

/**
 * Created by said on 13/12/15.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class Walker {

    private FileSystem fileSystem;
    private List<Path> inputPathsList;
    private String skipFileName = ".DS_Store";

    public Walker(String baseURIString, Configuration conf) throws IOException{
        this.fileSystem = FileSystem.get(URI.create(baseURIString), conf);
        this.inputPathsList = new ArrayList<Path>();
    }

    public Path[] walkFromRoot(String baseDataDir) throws Exception{

        Path baseDataDirectoryPath = new Path(baseDataDir);
        FileStatus[] list = this.fileSystem.listStatus(baseDataDirectoryPath);

        if(null != list){
            for ( FileStatus personDirectory : list ) {
                if(personDirectory.isDirectory()){
                    this.walk(personDirectory.getPath());
                }
            }
        }

        return (Path[])inputPathsList.toArray(new Path[inputPathsList.size()]);
    }

    public void walk( Path subPath) throws Exception {

        FileStatus[] subFilesList = this.fileSystem.listStatus(subPath);

        if (subFilesList != null) {
            for (FileStatus aFileStatus : subFilesList) {

                if (!aFileStatus.isDirectory()) {
                    String nameOfFile = aFileStatus.getPath().getName();

                    if(!nameOfFile.equals(this.skipFileName)){
                        this.inputPathsList.add(aFileStatus.getPath());
                    }

                } else {
                    walk(aFileStatus.getPath());
                }
            }
        }
    }
}
