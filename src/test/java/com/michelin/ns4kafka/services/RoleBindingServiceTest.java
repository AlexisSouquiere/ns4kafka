package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.ObjectMeta;
import com.michelin.ns4kafka.models.RoleBinding;
import com.michelin.ns4kafka.repositories.RoleBindingRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RoleBindingServiceTest {
    @Mock
    RoleBindingRepository roleBindingRepository;

    @InjectMocks
    RoleBindingService roleBindingService;

    @Test
    void findByName() {
        RoleBinding rb1 = RoleBinding.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace-rb1")
                        .cluster("local")
                        .build())
                .build();
        RoleBinding rb2 = RoleBinding.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace-rb2")
                        .cluster("local")
                        .build())
                .build();
        RoleBinding rb3 = RoleBinding.builder()
                .metadata(ObjectMeta.builder()
                        .name("namespace-rb3")
                        .cluster("local")
                        .build())
                .build();

        when(roleBindingRepository.findAllForNamespace("namespace")).thenReturn(List.of(rb1,rb2,rb3));

        var result = roleBindingService.findByName("namespace", "namespace-rb2");
        assertEquals(rb2, result.get());
    }
}
