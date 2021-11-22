package com.michelin.ns4kafka.kustomize;

import com.michelin.ns4kafka.kustomize.models.Resource;
import com.michelin.ns4kafka.kustomize.services.ConfigFileService;
import com.michelin.ns4kafka.kustomize.services.GeneratorService;
import picocli.CommandLine;

import javax.inject.Inject;
import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "build", description = "Generate kafkactl configs")
public class BuildSubcommand implements Callable<Integer> {


    @Inject
    ConfigFileService configLoaderService;

    @Inject
    GeneratorService generatorService;

    @CommandLine.ParentCommand
    public KustomizeCommand kustomizeCommand;
    @CommandLine.Option(names = {"-f", "--file"}, description = "YAML File or Directory containing YAML resources")
    public Optional<String> file;

    @Override
    public Integer call() {

        List<Resource> inputs;

        if (file.isPresent()) {
            inputs = configLoaderService.loadConfigsFiles(new File(file.get()));
            System.out.printf(inputs.toString());
            generatorService.generateResources(inputs);
            configLoaderService.generateConfigFile(new File(file.get()));
        }
        return 0;
    }
}
