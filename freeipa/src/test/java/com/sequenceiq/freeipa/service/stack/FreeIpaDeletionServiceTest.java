package com.sequenceiq.freeipa.service.stack;

import static com.sequenceiq.freeipa.flow.stack.termination.StackTerminationEvent.TERMINATION_EVENT;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.sequenceiq.freeipa.entity.Stack;
import com.sequenceiq.freeipa.flow.stack.termination.event.TerminationEvent;
import com.sequenceiq.freeipa.service.FreeIpaFlowManager;

@ExtendWith(MockitoExtension.class)
class FreeIpaDeletionServiceTest {

    private static final String ENVIRONMENT_CRN = "test:environment:crn";

    private static final Long STACK_ID = 1L;

    private static final String STACK_NAME = "stack-name";

    private static final String ACCOUNT_ID = "account:id";

    @InjectMocks
    private FreeIpaDeletionService underTest;

    @Mock
    private StackService stackService;

    @Mock
    private FreeIpaFlowManager flowManager;

    private Stack stack;

    @BeforeEach
    void setUp() {
        stack = new Stack();
        stack.setId(STACK_ID);
        stack.setName(STACK_NAME);
    }

    @Test
    void delete() {
        when(stackService.getByAccountIdEnvironmentAndName(eq(ACCOUNT_ID), eq(ENVIRONMENT_CRN), eq(STACK_NAME))).thenReturn(stack);

        underTest.delete(ACCOUNT_ID, ENVIRONMENT_CRN, STACK_NAME);

        verify(stackService, times(1)).getByAccountIdEnvironmentAndName(eq(ACCOUNT_ID), eq(ENVIRONMENT_CRN), eq(STACK_NAME));

        ArgumentCaptor<TerminationEvent> terminationEventArgumentCaptor = ArgumentCaptor.forClass(TerminationEvent.class);
        verify(flowManager, times(1)).notify(eq(TERMINATION_EVENT.event()), terminationEventArgumentCaptor.capture());

        assertAll(
                () -> assertEquals(TERMINATION_EVENT.event(), terminationEventArgumentCaptor.getValue().selector()),
                () -> assertEquals(STACK_ID, terminationEventArgumentCaptor.getValue().getResourceId()),
                () -> assertFalse(terminationEventArgumentCaptor.getValue().getForced())
        );
    }

    @Test
    void deleteByCrn() {
        when(stackService.getByEnvironmentCrn(eq(ENVIRONMENT_CRN))).thenReturn(stack);

        underTest.delete(ENVIRONMENT_CRN);

        verify(stackService, times(1)).getByEnvironmentCrn(eq(ENVIRONMENT_CRN));

        ArgumentCaptor<TerminationEvent> terminationEventArgumentCaptor = ArgumentCaptor.forClass(TerminationEvent.class);
        verify(flowManager, times(1)).notify(eq(TERMINATION_EVENT.event()), terminationEventArgumentCaptor.capture());

        assertAll(
                () -> assertEquals(TERMINATION_EVENT.event(), terminationEventArgumentCaptor.getValue().selector()),
                () -> assertEquals(STACK_ID, terminationEventArgumentCaptor.getValue().getResourceId()),
                () -> assertFalse(terminationEventArgumentCaptor.getValue().getForced())
        );
    }
}