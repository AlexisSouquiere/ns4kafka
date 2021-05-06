package com.michelin.ns4kafka.controllers;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.services.AccessControlEntryService;
import com.michelin.ns4kafka.services.NamespaceService;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.*;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;

import javax.inject.Inject;
import javax.validation.Valid;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Tag(name = "Cross Namespace Topic Grants",
        description = "APIs to handle cross namespace ACL")
@Controller("/api/namespaces/{namespace}/acls")
public class AccessControlListController extends NamespacedResourceController {
    @Inject
    NamespaceService namespaceService;
    @Inject
    AccessControlEntryService accessControlEntryService;

    @Operation(summary = "Returns the Access Control Entry List")
    @Get("{?limit}")
    public List<AccessControlEntry> list(String namespace, Optional<AclLimit> limit) {
        if (limit.isEmpty())
            limit = Optional.of(AclLimit.ALL);

        Namespace ns = getNamespace(namespace);

        switch (limit.get()) {
            case GRANTEE:
                return accessControlEntryService.findAllGrantedToNamespace(ns)
                        .stream()
                        // granted to me
                        .filter(accessControlEntry -> accessControlEntry.getSpec().getGrantedTo().equals(namespace))
                        .sorted(Comparator.comparing(o -> o.getMetadata().getNamespace()))
                        .collect(Collectors.toList());
            case GRANTOR:
                return accessControlEntryService.findAllForCluster(ns.getMetadata().getCluster())
                        .stream()
                        // granted by me
                        .filter(accessControlEntry -> accessControlEntry.getMetadata().getNamespace().equals(namespace))
                        // without the granted to me
                        .filter(accessControlEntry -> !accessControlEntry.getSpec().getGrantedTo().equals(namespace))
                        .sorted(Comparator.comparing(o -> o.getSpec().getGrantedTo()))
                        .collect(Collectors.toList());
            case ALL:
            default:
                return accessControlEntryService.findAllForCluster(ns.getMetadata().getCluster())
                        .stream()
                        .filter(accessControlEntry ->
                                accessControlEntry.getMetadata().getNamespace().equals(namespace)
                                        || accessControlEntry.getSpec().getGrantedTo().equals(namespace)
                        )
                        .sorted(Comparator.comparing(o -> o.getMetadata().getNamespace()))
                        .collect(Collectors.toList());
        }

    }

    @Post("{?dryrun}")
    public AccessControlEntry apply(String namespace, @Valid @Body AccessControlEntry accessControlEntry, @QueryValue(defaultValue = "false") boolean dryrun) {

        boolean isAdmin = namespace.equals(Namespace.ADMIN_NAMESPACE);
        if (!isAdmin) {
            Namespace ns = getNamespace(namespace);
            return applyAsUser(ns, accessControlEntry, dryrun);

        } else {
            return applyAsAdmin(accessControlEntry, dryrun);
        }
    }

    public AccessControlEntry applyAsUser(Namespace namespace, AccessControlEntry accessControlEntry, boolean dryRun) {
        //validation
        List<String> validationErrors = accessControlEntryService.validate(accessControlEntry, namespace);
        if (!validationErrors.isEmpty()) {
            throw new ResourceValidationException(validationErrors);
        }
        //augment
        accessControlEntry.getMetadata().setCluster(namespace.getMetadata().getCluster());
        accessControlEntry.getMetadata().setNamespace(namespace.getMetadata().getName());
        //dryrun checks
        if (dryRun) {
            return accessControlEntry;
        }
        //store
        return accessControlEntryService.create(accessControlEntry);
    }

    public AccessControlEntry applyAsAdmin(AccessControlEntry accessControlEntry, boolean dryRun) {
        //validation
        List<String> validationErrors = accessControlEntryService.validateAsAdmin(accessControlEntry);
        if (!validationErrors.isEmpty()) {
            throw new ResourceValidationException(validationErrors);
        }
        //augment
        accessControlEntry.getMetadata().setNamespace(Namespace.ADMIN_NAMESPACE);
        //dryrun checks
        if (dryRun) {
            return accessControlEntry;
        }
        //store
        return accessControlEntryService.create(accessControlEntry);
    }

    @Delete("/{name}{?dryrun}")
    @Status(HttpStatus.NO_CONTENT)
    public HttpResponse<Void> delete(String namespace, String name, @QueryValue(defaultValue = "false") boolean dryrun) {
        // 1. Check ACL exists
        // 2. Check Ownership of ACL using metadata.namespace
        // 3. Drop ACL
        Optional<AccessControlEntry> existingAccessControlEntry = accessControlEntryService.findByName(namespace, name);

        if (existingAccessControlEntry.isEmpty()) {
            throw new ResourceValidationException(List.of("Invalid value " + name + " for name : AccessControlEntry doesn't exist in this namespace"));
        }

        if (dryrun) {
            return HttpResponse.noContent();
        }

        accessControlEntryService.delete(existingAccessControlEntry.get());

        return HttpResponse.noContent();
    }

    public enum AclLimit {
        /**
         * Returns all ACL
         */
        ALL,
        GRANTOR,
        GRANTEE
    }
}