package com.sequenceiq.cloudbreak.controller.validation.environment;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.Sets;
import com.sequenceiq.cloudbreak.TestUtil;
import com.sequenceiq.cloudbreak.api.model.ldap.LdapConfigRequest;
import com.sequenceiq.cloudbreak.api.model.rds.RDSConfigRequest;
import com.sequenceiq.cloudbreak.api.model.stack.cluster.ClusterRequest;
import com.sequenceiq.cloudbreak.controller.validation.ValidationResult;
import com.sequenceiq.cloudbreak.domain.LdapConfig;
import com.sequenceiq.cloudbreak.domain.ProxyConfig;
import com.sequenceiq.cloudbreak.domain.RDSConfig;
import com.sequenceiq.cloudbreak.domain.stack.Stack;
import com.sequenceiq.cloudbreak.domain.view.EnvironmentView;
import com.sequenceiq.cloudbreak.domain.workspace.Workspace;
import com.sequenceiq.cloudbreak.service.ldapconfig.LdapConfigService;
import com.sequenceiq.cloudbreak.service.proxy.ProxyConfigService;
import com.sequenceiq.cloudbreak.service.rdsconfig.RdsConfigService;

@RunWith(MockitoJUnitRunner.class)
public class ClusterCreationEnvironmentValidatorTest {
    @Mock
    private ProxyConfigService proxyConfigService;

    @Mock
    private LdapConfigService ldapConfigService;

    @Mock
    private RdsConfigService rdsConfigService;

    @InjectMocks
    private ClusterCreationEnvironmentValidator underTest;

    @Test
    public void testValidateShouldBeSuccessWhenStackRegionIsValidAndEnvironmentsResourcesAreNotGiven() {
        // GIVEN
        Stack stack = getStack();
        ClusterRequest clusterRequest = new ClusterRequest();
        // WHEN
        ValidationResult actualResult = underTest.validate(clusterRequest, stack);
        // THEN
        Assert.assertFalse(actualResult.hasError());
    }

    @Test
    public void testValidateShouldBeSuccessWhenNoEnvironmentProvided() {
        // GIVEN
        Stack stack = getStack();
        stack.setEnvironment(null);
        ClusterRequest clusterRequest = new ClusterRequest();
        ProxyConfig proxyConfig = createProxyConfig("proxy", Sets.newHashSet());
        clusterRequest.setProxyName(proxyConfig.getName());
        Mockito.when(proxyConfigService.getByNameForWorkspaceId(proxyConfig.getName(), stack.getWorkspace().getId())).thenReturn(proxyConfig);
        LdapConfig ldapConfig = createLdapConfig("ldap", Sets.newHashSet());
        Mockito.when(ldapConfigService.getByNameForWorkspaceId(ldapConfig.getName(), stack.getWorkspace().getId())).thenReturn(ldapConfig);
        clusterRequest.setLdapConfigName("ldap");
        RDSConfig rdsConfig1 = createRdsConfig("rds1", Sets.newHashSet());
        Mockito.when(rdsConfigService.getByNameForWorkspaceId(rdsConfig1.getName(), stack.getWorkspace().getId())).thenReturn(rdsConfig1);
        RDSConfig rdsConfig2 = createRdsConfig("rds2", Sets.newHashSet());
        Mockito.when(rdsConfigService.getByNameForWorkspaceId(rdsConfig2.getName(), stack.getWorkspace().getId())).thenReturn(rdsConfig2);
        clusterRequest.setRdsConfigNames(Sets.newHashSet(rdsConfig1.getName(), rdsConfig2.getName()));
        RDSConfig rdsConfig3 = createRdsConfig("rds3", Sets.newHashSet());
        Mockito.when(rdsConfigService.get(rdsConfig3.getId())).thenReturn(rdsConfig3);
        RDSConfig rdsConfig4 = createRdsConfig("rds4", Sets.newHashSet());
        Mockito.when(rdsConfigService.get(rdsConfig4.getId())).thenReturn(rdsConfig4);
        clusterRequest.setRdsConfigIds(Sets.newHashSet(rdsConfig3.getId(), rdsConfig4.getId()));
        clusterRequest.setRdsConfigJsons(Sets.newHashSet(createRdsConfigRequest("rds5", Sets.newHashSet()),
                createRdsConfigRequest("rds5", Sets.newHashSet())));
        // WHEN
        ValidationResult actualResult = underTest.validate(clusterRequest, stack);
        // THEN
        Assert.assertFalse(actualResult.hasError());
    }

    @Test
    public void testValidateShouldBeSuccessWhenResourcesAreInTheSameEnvironmentOrGlobals() {
        // GIVEN
        Stack stack = getStack();
        ClusterRequest clusterRequest = new ClusterRequest();
        ProxyConfig proxyConfig = createProxyConfig("proxy", Sets.newHashSet("env1", "env2"));
        clusterRequest.setProxyName(proxyConfig.getName());
        Mockito.when(proxyConfigService.getByNameForWorkspaceId(proxyConfig.getName(), stack.getWorkspace().getId())).thenReturn(proxyConfig);
        LdapConfig ldapConfig = createLdapConfig("ldap", Sets.newHashSet());
        Mockito.when(ldapConfigService.getByNameForWorkspaceId(ldapConfig.getName(), stack.getWorkspace().getId())).thenReturn(ldapConfig);
        clusterRequest.setLdapConfigName("ldap");
        RDSConfig rdsConfig1 = createRdsConfig("rds1", Sets.newHashSet());
        Mockito.when(rdsConfigService.getByNameForWorkspaceId(rdsConfig1.getName(), stack.getWorkspace().getId())).thenReturn(rdsConfig1);
        RDSConfig rdsConfig2 = createRdsConfig("rds2", Sets.newHashSet("env1", "env3"));
        Mockito.when(rdsConfigService.getByNameForWorkspaceId(rdsConfig2.getName(), stack.getWorkspace().getId())).thenReturn(rdsConfig2);
        clusterRequest.setRdsConfigNames(Sets.newHashSet(rdsConfig1.getName(), rdsConfig2.getName()));
        RDSConfig rdsConfig3 = createRdsConfig("rds3", Sets.newHashSet("env1", "env2"));
        Mockito.when(rdsConfigService.get(rdsConfig3.getId())).thenReturn(rdsConfig3);
        RDSConfig rdsConfig4 = createRdsConfig("rds4", Sets.newHashSet("env1", "env5"));
        Mockito.when(rdsConfigService.get(rdsConfig4.getId())).thenReturn(rdsConfig4);
        clusterRequest.setRdsConfigIds(Sets.newHashSet(rdsConfig3.getId(), rdsConfig4.getId()));
        clusterRequest.setRdsConfigJsons(Sets.newHashSet(createRdsConfigRequest("rds5", Sets.newHashSet()),
                createRdsConfigRequest("rds5", Sets.newHashSet("env1", "env6"))));
        // WHEN
        ValidationResult actualResult = underTest.validate(clusterRequest, stack);
        // THEN
        Assert.assertFalse(actualResult.hasError());
    }

    @Test
    public void testValidateShouldBeFailedWhenStackRegionIsInvalidAndEnvironmentsResourcesAreNotInGoodEnvironment() {
        // GIVEN
        Stack stack = getStack();
        stack.setRegion("region3");
        ClusterRequest clusterRequest = new ClusterRequest();
        ProxyConfig proxyConfig = createProxyConfig("proxy", Sets.newHashSet("env2", "env3"));
        clusterRequest.setProxyName(proxyConfig.getName());
        Mockito.when(proxyConfigService.getByNameForWorkspaceId(proxyConfig.getName(), stack.getWorkspace().getId())).thenReturn(proxyConfig);
        LdapConfig ldapConfig = createLdapConfig("ldap", Sets.newHashSet());
        Mockito.when(ldapConfigService.getByNameForWorkspaceId(ldapConfig.getName(), stack.getWorkspace().getId())).thenReturn(ldapConfig);
        clusterRequest.setLdapConfigName("ldap");
        RDSConfig rdsConfig3 = createRdsConfig("rds1", Sets.newHashSet("env2", "env3"));
        Mockito.when(rdsConfigService.get(rdsConfig3.getId())).thenReturn(rdsConfig3);
        RDSConfig rdsConfig4 = createRdsConfig("rds2", Sets.newHashSet("env4", "env5"));
        Mockito.when(rdsConfigService.get(rdsConfig4.getId())).thenReturn(rdsConfig4);
        RDSConfig rdsConfig1 = createRdsConfig("rds3", Sets.newHashSet());
        Mockito.when(rdsConfigService.getByNameForWorkspaceId(rdsConfig1.getName(), stack.getWorkspace().getId())).thenReturn(rdsConfig1);
        RDSConfig rdsConfig2 = createRdsConfig("rds4", Sets.newHashSet("env2", "env3"));
        Mockito.when(rdsConfigService.getByNameForWorkspaceId(rdsConfig2.getName(), stack.getWorkspace().getId())).thenReturn(rdsConfig2);
        clusterRequest.setRdsConfigNames(Sets.newLinkedHashSet(Arrays.asList(rdsConfig1.getName(), rdsConfig2.getName())));
        clusterRequest.setRdsConfigIds(Sets.newLinkedHashSet(Arrays.asList(rdsConfig3.getId(), rdsConfig4.getId())));
        clusterRequest.setRdsConfigJsons(Sets.newLinkedHashSet(Arrays.asList(createRdsConfigRequest("rds5", Sets.newHashSet()),
                createRdsConfigRequest("rds6", Sets.newHashSet("env5", "env6")))));
        // WHEN
        ValidationResult actualResult = underTest.validate(clusterRequest, stack);
        // THEN
        Assert.assertTrue(actualResult.hasError());
        Assert.assertEquals(6, actualResult.getErrors().size());
        Assert.assertEquals("[region3] region is not enabled in [env1] environment. Enabled environments: [region1,region2]", actualResult.getErrors().get(0));
        Assert.assertEquals("Stack cannot use proxy ProxyConfig resource which is not attached to env1 environment and not global.",
                actualResult.getErrors().get(1));
        Assert.assertEquals("Stack cannot use rds1 RDSConfig resource which is not attached to env1 environment and not global.",
                actualResult.getErrors().get(2));
        Assert.assertEquals("Stack cannot use rds2 RDSConfig resource which is not attached to env1 environment and not global.",
                actualResult.getErrors().get(3));
        Assert.assertEquals("Stack cannot use rds4 RDSConfig resource which is not attached to env1 environment and not global.",
                actualResult.getErrors().get(4));
        Assert.assertEquals("Stack cannot use rds6 RDSConfig resource which is not attached to env1 environment and not global.",
                actualResult.getErrors().get(5));
    }

    @Test
    public void testValidateShouldBeFailedWhenStackEnvIsNullButEnvironmentsResourcesAreNotGlobals() {
        // GIVEN
        Stack stack = getStack();
        stack.setEnvironment(null);
        ClusterRequest clusterRequest = new ClusterRequest();
        ProxyConfig proxyConfig = createProxyConfig("proxy", Sets.newHashSet("env2", "env3"));
        clusterRequest.setProxyName(proxyConfig.getName());
        Mockito.when(proxyConfigService.getByNameForWorkspaceId(proxyConfig.getName(), stack.getWorkspace().getId())).thenReturn(proxyConfig);
        LdapConfig ldapConfig = createLdapConfig("ldap", Sets.newHashSet());
        Mockito.when(ldapConfigService.getByNameForWorkspaceId(ldapConfig.getName(), stack.getWorkspace().getId())).thenReturn(ldapConfig);
        clusterRequest.setLdapConfigName("ldap");
        RDSConfig rdsConfig3 = createRdsConfig("rds1", Sets.newHashSet("env2", "env3"));
        Mockito.when(rdsConfigService.get(rdsConfig3.getId())).thenReturn(rdsConfig3);
        RDSConfig rdsConfig4 = createRdsConfig("rds2", Sets.newHashSet("env4", "env5"));
        Mockito.when(rdsConfigService.get(rdsConfig4.getId())).thenReturn(rdsConfig4);
        RDSConfig rdsConfig1 = createRdsConfig("rds3", Sets.newHashSet());
        Mockito.when(rdsConfigService.getByNameForWorkspaceId(rdsConfig1.getName(), stack.getWorkspace().getId())).thenReturn(rdsConfig1);
        RDSConfig rdsConfig2 = createRdsConfig("rds4", Sets.newHashSet("env2", "env3"));
        Mockito.when(rdsConfigService.getByNameForWorkspaceId(rdsConfig2.getName(), stack.getWorkspace().getId())).thenReturn(rdsConfig2);
        clusterRequest.setRdsConfigNames(Sets.newLinkedHashSet(Arrays.asList(rdsConfig1.getName(), rdsConfig2.getName())));
        clusterRequest.setRdsConfigIds(Sets.newLinkedHashSet(Arrays.asList(rdsConfig3.getId(), rdsConfig4.getId())));
        clusterRequest.setRdsConfigJsons(Sets.newLinkedHashSet(Arrays.asList(createRdsConfigRequest("rds5", Sets.newHashSet()),
                createRdsConfigRequest("rds6", Sets.newHashSet("env5", "env6")))));
        // WHEN
        ValidationResult actualResult = underTest.validate(clusterRequest, stack);
        // THEN
        Assert.assertTrue(actualResult.hasError());
        Assert.assertEquals(5, actualResult.getErrors().size());
        Assert.assertEquals("Stack without environment cannot use proxy ProxyConfig resource which attached to an environment.",
                actualResult.getErrors().get(0));
        Assert.assertEquals("Stack without environment cannot use rds1 RDSConfig resource which attached to an environment.",
                actualResult.getErrors().get(1));
        Assert.assertEquals("Stack without environment cannot use rds2 RDSConfig resource which attached to an environment.",
                actualResult.getErrors().get(2));
        Assert.assertEquals("Stack without environment cannot use rds4 RDSConfig resource which attached to an environment.",
                actualResult.getErrors().get(3));
        Assert.assertEquals("Stack without environment cannot use rds6 RDSConfig resource which attached to an environment.",
                actualResult.getErrors().get(4));
    }

    @Test
    public void testValidateShouldBeFailedWhenLdapConfigGivenWithIdIsNotInGoodEnvironment() {
        // GIVEN
        Stack stack = getStack();
        ClusterRequest clusterRequest = new ClusterRequest();
        LdapConfig ldapConfig = createLdapConfig("ldap", Sets.newHashSet("env2", "env3"));
        Mockito.when(ldapConfigService.get(ldapConfig.getId())).thenReturn(ldapConfig);
        clusterRequest.setLdapConfigId(ldapConfig.getId());
        // WHEN
        ValidationResult actualResult = underTest.validate(clusterRequest, stack);
        // THEN
        Assert.assertTrue(actualResult.hasError());
        Assert.assertEquals(1, actualResult.getErrors().size());
        Assert.assertEquals("Stack cannot use ldap LdapConfig resource which is not attached to env1 environment and not global.",
                actualResult.getErrors().get(0));
    }

    @Test
    public void testValidateShouldBeFailedWhenLdapConfigGivenWithRequestIsNotInGoodEnvironment() {
        // GIVEN
        Stack stack = getStack();
        ClusterRequest clusterRequest = new ClusterRequest();
        LdapConfigRequest ldapConfigRequest = new LdapConfigRequest();
        ldapConfigRequest.setName("ldap");
        ldapConfigRequest.setEnvironments(Sets.newHashSet("env2", "env3"));
        clusterRequest.setLdapConfig(ldapConfigRequest);
        // WHEN
        ValidationResult actualResult = underTest.validate(clusterRequest, stack);
        // THEN
        Assert.assertTrue(actualResult.hasError());
        Assert.assertEquals(1, actualResult.getErrors().size());
        Assert.assertEquals("Stack cannot use ldap LdapConfig resource which is not attached to env1 environment and not global.",
                actualResult.getErrors().get(0));
    }

    private Stack getStack() {
        Stack stack = new Stack();
        stack.setRegion("region1");
        Workspace workspace = new Workspace();
        workspace.setId(1L);
        stack.setWorkspace(workspace);
        EnvironmentView environmentView = new EnvironmentView();
        environmentView.setName("env1");
        environmentView.setRegionsSet(Sets.newHashSet("region1", "region2"));
        stack.setEnvironment(environmentView);
        return stack;
    }

    private LdapConfig createLdapConfig(String name, Set<String> environments) {
        LdapConfig ldapConfig = new LdapConfig();
        ldapConfig.setId(TestUtil.generateUniqueId());
        ldapConfig.setName(name);
        ldapConfig.setEnvironments(environments.stream().map(env -> createEnvironmentView(env)).collect(Collectors.toSet()));
        return ldapConfig;
    }

    private ProxyConfig createProxyConfig(String name, Set<String> environments) {
        ProxyConfig proxyConfig = new ProxyConfig();
        proxyConfig.setId(TestUtil.generateUniqueId());
        proxyConfig.setName(name);
        proxyConfig.setEnvironments(environments.stream().map(env -> createEnvironmentView(env)).collect(Collectors.toSet()));
        return proxyConfig;
    }

    private RDSConfigRequest createRdsConfigRequest(String name, Set<String> environments) {
        RDSConfigRequest rdsConfigRequest = new RDSConfigRequest();
        rdsConfigRequest.setName(name);
        rdsConfigRequest.setEnvironments(environments);
        return rdsConfigRequest;
    }

    private RDSConfig createRdsConfig(String name, Set<String> environments) {
        RDSConfig rdsConfig = new RDSConfig();
        rdsConfig.setId(TestUtil.generateUniqueId());
        rdsConfig.setName(name);
        rdsConfig.setEnvironments(environments.stream().map(env -> createEnvironmentView(env)).collect(Collectors.toSet()));
        return rdsConfig;
    }

    private EnvironmentView createEnvironmentView(String name) {
        EnvironmentView env = new EnvironmentView();
        env.setId(TestUtil.generateUniqueId());
        env.setName(name);
        return env;
    }
}
