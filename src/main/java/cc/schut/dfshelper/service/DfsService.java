package cc.schut.dfshelper.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import javax.annotation.PostConstruct;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Date;

@Component
public class DfsService {
    private static Logger LOG = LoggerFactory.getLogger(DfsService.class);

    private boolean dryRun = false;
    private boolean verbose = false;
    private FileSystem fileSystem;

    @PostConstruct
    public void init() throws IOException {
        Configuration conf = new Configuration();
        conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
        conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hadoop.security.authorization", "true");
        UserGroupInformation.setConfiguration(conf);
        fileSystem = FileSystem.get(conf);
    }

    public void setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    public void deleteDfs(Path path, long olderThan) throws IOException {
        if (verbose) {
            LOG.info("Starting dfs scan on path {}", path);
        }
        RemoteIterator<LocatedFileStatus> it = fileSystem.listLocatedStatus(path);
        while (it.hasNext()) {
            LocatedFileStatus lfs = it.next();

            Format format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String lastModified = format.format(new Date(lfs.getModificationTime()));
            LocalDate now = LocalDate.now();
            LocalDate epoch = Instant.ofEpochMilli(lfs.getModificationTime())
                    .atZone(ZoneId.systemDefault()).toLocalDate();
            long daysBetween = ChronoUnit.DAYS.between(epoch, now);

            long objectCount = 0;
            if (lfs.isDirectory()) {
                try {
                    ContentSummary contentSummary = fileSystem.getContentSummary(lfs.getPath());
                    objectCount = contentSummary.getDirectoryCount() + contentSummary.getFileCount();
                } catch (FileNotFoundException exception) {
                    LOG.info("File not found {}", lfs.getPath());
                }
                if (objectCount > 1 && daysBetween > olderThan) {
                    // Recursively walk through directories deleting old files.
                    deleteDfs(lfs.getPath(), olderThan);

                    // After we are done recount because it might now be empty.
                    ContentSummary contentSummary = fileSystem.getContentSummary(lfs.getPath());
                    objectCount = contentSummary.getDirectoryCount() + contentSummary.getFileCount();
                }
            }

            // doing the actual delete
            if (daysBetween > olderThan && (objectCount == 1 || objectCount == 0)) {
                LOG.info(String.format("Deleting %s, %s , %s , %s days old",
                        lfs.isDirectory() ? "d:" + objectCount : "f",
                        lfs.getPath().toUri(),
                        lastModified,
                        daysBetween)
                );
                if (!dryRun) {
                    fileSystem.delete(lfs.getPath(), Boolean.FALSE);
                }
            } else {
                if (verbose) {
                    LOG.info("Not deleting {} count:{}", lfs.getPath(), objectCount);
                }
            }
        }
        if (verbose) {
            LOG.info("Finished dfs scan on path {}", path);
        }
    }
}
