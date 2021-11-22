package com.michelin.ns4kafka.kustomize.services;

import com.michelin.ns4kafka.kustomize.models.GeneratedResources;
import com.michelin.ns4kafka.kustomize.models.Resource;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;

import javax.inject.Singleton;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.yaml.snakeyaml.DumperOptions.FlowStyle;


@Singleton
public class ConfigFileService {

    public List<Resource> loadConfigsFiles(File configDir) {

        File[] files = configDir.listFiles(f -> f.isFile() && (f.getName().endsWith(".yaml") || f.getName().endsWith(".yml")));

        List<Resource> configs = Arrays.stream(files)
                .filter(File::isFile)
                .map(File::toPath)
                .flatMap(path -> {
                    try {
                        String content = Files.readString(path);
                        Yaml yaml = new Yaml(new Constructor(Resource.class));
                        return StreamSupport.stream(yaml.loadAll(content).spliterator(), false)
                                .map(r -> (Resource) r);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toList());
        return configs;
    }

    public void generateConfigFile(File configDir) {

        String outputDir = configDir.getAbsolutePath() + "/resources";
        new File(outputDir).mkdir();

        GeneratedResources.resources.keySet().stream().forEach(
                envName -> {
                    String envDir = outputDir + "/" + envName;
                    new File(envDir).mkdir();
                    generateResources(GeneratedResources.resources.get(envName).get(GeneratedResources.Kind.TOPICS), envDir + "/topics.yml");
                    generateResources(GeneratedResources.resources.get(envName).get(GeneratedResources.Kind.CONNECTORS), envDir + "/connectors.yml");
                    generateResources(GeneratedResources.resources.get(envName).get(GeneratedResources.Kind.STREAMS), envDir + "/streams.yml");
                }
        );
    }

    private void generateResources(List<Resource> resources, String output) {
        try {
            File file = new File(output);
            file.createNewFile();
            PrintWriter writer = new PrintWriter(file);
            DumperOptions dumperOptions = new DumperOptions();
            dumperOptions.setPrettyFlow(true);
            dumperOptions.setDefaultFlowStyle(FlowStyle.BLOCK);
            Representer representer = new Representer();
            representer.addClassTag(Resource.class, Tag.MAP);
            Yaml yaml = new Yaml(representer,dumperOptions);
            writer.write("---");
            writer.println();
            for (Resource r : resources) {
                yaml.dump(r, writer);
                writer.println();
                writer.write("---");
                writer.println();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
