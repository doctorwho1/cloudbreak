package com.sequenceiq.it.cloudbreak.newway.testcase.mock;

import static com.sequenceiq.it.cloudbreak.newway.assertion.kerberos.KerberosTestAssertion.validateCustomDomain;
import static com.sequenceiq.it.cloudbreak.newway.mock.model.ClouderaManagerMock.API_ROOT;

import java.math.BigDecimal;

import javax.inject.Inject;

import org.springframework.http.HttpMethod;
import org.testng.annotations.Test;

import com.cloudera.api.swagger.model.ApiCommand;
import com.cloudera.api.swagger.model.ApiConfigList;
import com.sequenceiq.it.cloudbreak.newway.assertion.MockVerification;
import com.sequenceiq.it.cloudbreak.newway.client.KerberosTestClient;
import com.sequenceiq.it.cloudbreak.newway.client.StackTestClient;
import com.sequenceiq.it.cloudbreak.newway.context.Description;
import com.sequenceiq.it.cloudbreak.newway.context.MockedTestContext;
import com.sequenceiq.it.cloudbreak.newway.dto.ClusterTestDto;
import com.sequenceiq.it.cloudbreak.newway.dto.kerberos.ActiveDirectoryKerberosDescriptorTestDto;
import com.sequenceiq.it.cloudbreak.newway.dto.kerberos.FreeIPAKerberosDescriptorTestDto;
import com.sequenceiq.it.cloudbreak.newway.dto.kerberos.KerberosTestDto;
import com.sequenceiq.it.cloudbreak.newway.dto.stack.StackTestDto;
import com.sequenceiq.it.cloudbreak.newway.mock.model.SaltMock;
import com.sequenceiq.it.cloudbreak.newway.testcase.AbstractIntegrationTest;
import com.sequenceiq.it.spark.DynamicRouteStack;

public class KerberizedStackCreationTest extends AbstractIntegrationTest {

    @Inject
    private KerberosTestClient kerberosTestClient;

    @Inject
    private StackTestClient stackTestClient;

    @Test(dataProvider = TEST_CONTEXT_WITH_MOCK)
    @Description(
            given = "an ActiveDirectory based kerberos config without realm and domain",
            when = "calling a stack creation with that kerberos config",
            then = "custom should be used and salt action should be called")
    public void testCreationWitKerberosAndStackWithoutCustomDomainAndADWithoutDomainAndWithRealm(MockedTestContext testContext) {
        DynamicRouteStack dynamicRouteStack = testContext.getModel().getClouderaManagerMock().getDynamicRouteStack();
        dynamicRouteStack.put(API_ROOT + "/cm/config", (request, response) -> new ApiConfigList());
        dynamicRouteStack.post(API_ROOT + "/cm/commands/importAdminCredentials", (request, response) -> new ApiCommand().id(new BigDecimal(1)));
        testContext
                .given(ActiveDirectoryKerberosDescriptorTestDto.class)
                .withDomain(null)
                .withRealm("realm.addomain.com")
                .given(KerberosTestDto.class)
                .withActiveDirectoryDescriptor()
                .when(kerberosTestClient.createV4())
                .given(ClusterTestDto.class)
                .withKerberos()
                .given(StackTestDto.class)
                .withCluster()
                .when(stackTestClient.createV4())
                .await(STACK_AVAILABLE)
                .then(validateCustomDomain("realm.addomain.com"))
                .then(MockVerification.verify(HttpMethod.POST, SaltMock.SALT_ACTION_DISTRIBUTE).exactTimes(1).bodyContains(".realm.addomain.com", 5))
                .validate();
    }

    @Test(dataProvider = TEST_CONTEXT_WITH_MOCK)
    @Description(
            given = "an ActiveDirectory based kerberos config without realm and with domain",
            when = "calling a stack creation with that kerberos config",
            then = "propagated domain should be used and salt action should be called")
    public void testCreationWitKerberosAndStackWithoutCustomDomainAndADWithDomainAndWithRealm(MockedTestContext testContext) {
        DynamicRouteStack dynamicRouteStack = testContext.getModel().getClouderaManagerMock().getDynamicRouteStack();
        dynamicRouteStack.put(API_ROOT + "/cm/config", (request, response) -> new ApiConfigList());
        dynamicRouteStack.post(API_ROOT + "/cm/commands/importAdminCredentials", (request, response) -> new ApiCommand().id(new BigDecimal(1)));
        testContext
                .given(ActiveDirectoryKerberosDescriptorTestDto.class)
                .withDomain("custom.addomain.com")
                .withRealm("realm.addomain.com")
                .given(KerberosTestDto.class)
                .withActiveDirectoryDescriptor()
                .when(kerberosTestClient.createV4())
                .given(ClusterTestDto.class)
                .withKerberos()
                .given(StackTestDto.class)
                .withCluster()
                .when(stackTestClient.createV4())
                .await(STACK_AVAILABLE)
                .then(validateCustomDomain("custom.addomain.com"))
                .then(MockVerification.verify(HttpMethod.POST, SaltMock.SALT_ACTION_DISTRIBUTE).exactTimes(1).bodyContains(".custom.addomain.com", 5))
                .validate();
    }

    @Test(dataProvider = TEST_CONTEXT_WITH_MOCK)
    @Description(
            given = "a FreeIPA based kerberos config with realm and without domain",
            when = "calling a stack creation with that kerberos config",
            then = "propagated realm should be used and salt action should be called")
    public void testCreationWitKerberosAndStackWithoutCustomDomainAndFreeIPAWithoutDomainAndWithRealm(MockedTestContext testContext) {
        DynamicRouteStack dynamicRouteStack = testContext.getModel().getClouderaManagerMock().getDynamicRouteStack();
        dynamicRouteStack.put(API_ROOT + "/cm/config", (request, response) -> new ApiConfigList());
        testContext
                .given(FreeIPAKerberosDescriptorTestDto.class)
                .withDomain(null)
                .withRealm("realm.freeiparealm.com")
                .given(KerberosTestDto.class)
                .withFreeIPADescriptor()
                .when(kerberosTestClient.createV4())
                .given(ClusterTestDto.class)
                .withKerberos()
                .given(StackTestDto.class)
                .withCluster()
                .when(stackTestClient.createV4())
                .await(STACK_AVAILABLE)
                .then(validateCustomDomain("realm.freeiparealm.com"))
                .then(MockVerification.verify(HttpMethod.POST, SaltMock.SALT_ACTION_DISTRIBUTE).exactTimes(1).bodyContains(".realm.freeiparealm.com", 5))
                .validate();
    }

    @Test(dataProvider = TEST_CONTEXT_WITH_MOCK)
    @Description(
            given = "a FreeIPA based kerberos config with realm and domain",
            when = "calling a stack creation with that kerberos config",
            then = "propagated domain should be used and salt action should be called")
    public void testCreationWitKerberosAndStackWithoutCustomDomainAndFreeIPAWithDomainAndWithRealm(MockedTestContext testContext) {
        DynamicRouteStack dynamicRouteStack = testContext.getModel().getClouderaManagerMock().getDynamicRouteStack();
        dynamicRouteStack.put(API_ROOT + "/cm/config", (request, response) -> new ApiConfigList());

        testContext
                .given(FreeIPAKerberosDescriptorTestDto.class)
                .withDomain("custom.freeipadomain.com")
                .withRealm("realm.freeiparealm.com")
                .given(KerberosTestDto.class)
                .withFreeIPADescriptor()
                .when(kerberosTestClient.createV4())
                .given(ClusterTestDto.class)
                .withKerberos()
                .given(StackTestDto.class)
                .withCluster()
                .when(stackTestClient.createV4())
                .await(STACK_AVAILABLE)
                .then(validateCustomDomain("custom.freeipadomain.com"))
                .then(MockVerification.verify(HttpMethod.POST, SaltMock.SALT_ACTION_DISTRIBUTE).exactTimes(1).bodyContains(".custom.freeipadomain.com", 5))
                .validate();
    }
}
