package cc.schut.dfshelper;

import cc.schut.dfshelper.service.DfsService;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.Banner;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

@SpringBootApplication
public class MainApplication implements CommandLineRunner {

    @Autowired
    private DfsService dfsService;

    public static void main(String[] args) {
        new SpringApplicationBuilder(MainApplication.class)
                .bannerMode(Banner.Mode.OFF)
                .run(args);
    }

    @Override
    public void run(String... args) throws Exception {
        String path = null;
        String olderThan = null;
        for (String arg: args) {
            String[] nameValue = arg.split("=");
            switch (nameValue[0].toLowerCase()) {
                case "--path":
                    path = nameValue[1];
                    break;
                case "--olderthan":
                    olderThan = nameValue[1];
                    break;
                case "--threads":
                    dfsService.setThreads(Integer.valueOf(nameValue[1]));
                    break;
                case "--dryrun":
                    dfsService.setDryRun(Boolean.TRUE);
                    break;
                case "--verbose":
                    dfsService.setVerbose(Boolean.TRUE);
                    break;
                default:
                    System.out.println("Unknown parameter:" + nameValue[0]);
                    System.exit(2);
            }
        }
        if (path !=  null && olderThan != null) {
            dfsService.deleteDfs(new Path(path), Long.valueOf(olderThan));
        }
    }

}
