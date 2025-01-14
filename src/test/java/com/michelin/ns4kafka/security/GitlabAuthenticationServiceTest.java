package com.michelin.ns4kafka.security;

import com.michelin.ns4kafka.security.gitlab.GitlabApiClient;
import com.michelin.ns4kafka.security.gitlab.GitlabAuthenticationService;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MutableHttpResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(MockitoExtension.class)
class GitlabAuthenticationServiceTest {
    @Mock
    GitlabApiClient gitlabApiClient;

    @InjectMocks
    GitlabAuthenticationService gitlabAuthenticationService;

    @Test
    void findUserSuccess(){
        String token = "v4l1d_70k3n";
        Mockito.when(gitlabApiClient.findUser(token))
                .thenReturn(Mono.just(Map.of("user","test", "email", "user@mail.com")));

        Mono<String> authenticationResponsePublisher = gitlabAuthenticationService.findUsername(token);

        StepVerifier.create(authenticationResponsePublisher)
                .consumeNextWith(response -> assertEquals("user@mail.com", response))
                .verifyComplete();
    }

    @Test
    void findGroupsOnePage(){
        String token = "v4l1d_70k3n";
        MutableHttpResponse<List<Map<String, Object>>> pageOneResponse = HttpResponse
                .ok(List.of(
                        Map.<String, Object>of("full_path", "group1", "unusedKey", "unusedVal"),
                        Map.<String, Object>of("full_path", "group2", "unusedKey", "unusedVal")))
                .header("X-Total-Pages","1");

        Mockito.when(gitlabApiClient.getGroupsPage(token,1)).thenReturn(Flux.just(pageOneResponse));

        Flux<String> authenticationResponsePublisher = gitlabAuthenticationService.findAllGroups(token);

        StepVerifier.create(authenticationResponsePublisher)
            .consumeNextWith(response -> assertEquals("group1", response))
            .consumeNextWith(response -> assertEquals("group2", response))
            .verifyComplete();
    }

    @Test
    void findGroupsThreePages(){
        String token = "v4l1d_70k3n";
        MutableHttpResponse<List<Map<String, Object>>> pageOneResponse = HttpResponse
                .ok(List.of(
                        Map.<String, Object>of("full_path", "group1", "unusedKey", "unusedVal"),
                        Map.<String, Object>of("full_path", "group2", "unusedKey", "unusedVal")))
                .header("X-Next-Page","2")
                .header("X-Total-Pages","3");

        MutableHttpResponse<List<Map<String, Object>>> pageTwoResponse = HttpResponse
                .ok(List.of(
                        Map.<String, Object>of("full_path", "group3", "unusedKey", "unusedVal"),
                        Map.<String, Object>of("full_path", "group4", "unusedKey", "unusedVal")))
                .header("X-Next-Page","3")
                .header("X-Total-Pages","3");

        MutableHttpResponse<List<Map<String, Object>>> pageThreeResponse = HttpResponse
                .ok(List.of(
                        Map.<String, Object>of("full_path", "group5", "unusedKey", "unusedVal"),
                        Map.<String, Object>of("full_path", "group6", "unusedKey", "unusedVal")))
                .header("X-Total-Pages","3");

        Mockito.when(gitlabApiClient.getGroupsPage(token,1)).thenReturn(Flux.just(pageOneResponse));
        Mockito.when(gitlabApiClient.getGroupsPage(token,2)).thenReturn(Flux.just(pageTwoResponse));
        Mockito.when(gitlabApiClient.getGroupsPage(token,3)).thenReturn(Flux.just(pageThreeResponse));

        Publisher<String> authenticationResponsePublisher = gitlabAuthenticationService.findAllGroups(token);

        StepVerifier.create(authenticationResponsePublisher)
                .consumeNextWith(response -> assertEquals("group1", response))
                .consumeNextWith(response -> assertEquals("group2", response))
                .consumeNextWith(response -> assertEquals("group3", response))
                .consumeNextWith(response -> assertEquals("group4", response))
                .consumeNextWith(response -> assertEquals("group5", response))
                .consumeNextWith(response -> assertEquals("group6", response))
                .verifyComplete();
    }
}
