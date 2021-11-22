package com.michelin.ns4kafka.kustomize;

import java.util.concurrent.Callable;
import io.micronaut.configuration.picocli.PicocliRunner;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "kustomize",
        subcommands =
                {
                        BuildSubcommand.class
                },
        version = "1.0",
        mixinStandardHelpOptions = true)
public class KustomizeCommand implements Callable<Integer> {

    public static boolean VERBOSE = false;

    @Option(names = {"-v", "--verbose"}, description = "...")
    boolean verbose;

    public static void main(String[] args) {

        int exitCode = PicocliRunner.execute(KustomizeCommand.class, args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {
        CommandLine cmd = new CommandLine(new KustomizeCommand());
        cmd.usage(System.out);
        return 0;
    }
}
