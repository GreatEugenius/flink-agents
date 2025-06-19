package org.apache.flink.agents.runtime.operator;

import org.apache.flink.agents.plan.Action;
import org.apache.flink.agents.plan.Function;
import org.apache.flink.agents.plan.PythonFunction;
import org.apache.flink.agents.plan.WorkflowPlan;
import org.apache.flink.agents.runtime.backpressure.ThresholdBackPressureValve;
import org.apache.flink.agents.runtime.message.DataMessage;
import org.apache.flink.agents.runtime.message.PythonDataMessage;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.OutputTag;
import pemja.core.PythonInterpreter;
import pemja.core.PythonInterpreterConfig;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class FunctionGroupOperator<K> extends AbstractStreamOperator<DataMessage<K>>
        implements OneInputStreamOperator<DataMessage<K>, DataMessage<K>> {

    private static final long serialVersionUID = 1L;
    private final OutputTag<DataMessage<K>> outputTag;
    private final transient MailboxExecutor mailboxExecutor;

    private transient StreamRecord<DataMessage<K>> reused;
    private transient StreamRecord<DataMessage<K>> reusedOutput;

    private transient ThresholdBackPressureValve backPressureValve;
    private transient WorkflowPlan workflowPlan;
    private transient PythonInterpreter interpreter;

    private transient MapState<K, Integer> key2pendingActionCount;
    private transient ListState<DataMessage<K>> pendingInputEvents;
    private TypeInformation<K> keyTypeInfo;
    private TypeInformation<DataMessage<K>> dataMessageTypeInfo;

    public FunctionGroupOperator(
            OutputTag<DataMessage<K>> outputTag,
            MailboxExecutor mailboxExecutor,
            ChainingStrategy chainingStrategy,
            ProcessingTimeService processingTimeService,
            WorkflowPlan workflowPlan,
            TypeInformation<K> keyTypeInfo,
            TypeInformation<DataMessage<K>> dataMessageTypeInfo) {
        this.outputTag = outputTag;
        this.mailboxExecutor = Objects.requireNonNull(mailboxExecutor);
        this.chainingStrategy = chainingStrategy;
        this.processingTimeService = processingTimeService;
        this.workflowPlan = workflowPlan;
        this.keyTypeInfo = keyTypeInfo;
        this.dataMessageTypeInfo = dataMessageTypeInfo;
    }

    @Override
    public void processElement(StreamRecord<DataMessage<K>> record) throws Exception {
        while (backPressureValve.shouldBackPressure()) {
            mailboxExecutor.yield();
        }
        System.out.println("FunctionGroupOperator processElement " + record);
        DataMessage<K> dataMessage = record.getValue();
        K key = dataMessage.getKey();
        if (dataMessage.isInputEvent() && !canExecuteInputEvent(key)) {
            pendingInputEvents.add(dataMessage);
        }

        List<Action> actions = workflowPlan.getAction(dataMessage.getEventType());
        if (actions != null) {
            addPendingActionCount(dataMessage.getKey(), actions.size());
            for (Action action : actions) {
                DataMessage<K> actionOutputMessage = null;
                Function actionFunction = action.getFunc();
                if (actionFunction instanceof PythonFunction) {
                    actionOutputMessage =
                            executePythonFunction((PythonFunction) actionFunction, dataMessage);
                } else {
                    throw new RuntimeException("Unsupported action type: " + action.getClass());
                }
                System.out.println(
                        "FunctionGroupOperator processElement result " + actionOutputMessage);
                addPendingActionCount(key, -1);

                if (actionOutputMessage.isOutputEvent()) {
                    output.collect(outputTag, reusedOutput.replace(actionOutputMessage));
                } else {
                    List<Action> pengdingActions =
                            workflowPlan.getAction(actionOutputMessage.getEventType());
                    addPendingActionCount(
                            key, pengdingActions == null ? 0 : pengdingActions.size());
                    output.collect(reused.replace(actionOutputMessage));
                }
            }
        }
    }

    @Override
    public void open() throws Exception {
        super.open();

        Objects.requireNonNull(mailboxExecutor, "MailboxExecutor is unexpectedly NULL");

        reused = new StreamRecord<>(null);

        reusedOutput = new StreamRecord<>(null);

        this.backPressureValve = new ThresholdBackPressureValve(100);
        initInterpreter();

        MapStateDescriptor<K, Integer> key2pendingActionCountDescriptor =
                new MapStateDescriptor<>(
                        "key2pendingActionCount", keyTypeInfo, TypeInformation.of(Integer.class));
        key2pendingActionCount = getRuntimeContext().getMapState(key2pendingActionCountDescriptor);

        ListStateDescriptor<DataMessage<K>> pendingInputEventsDescriptor =
                new ListStateDescriptor<>("pendingInputEvents", dataMessageTypeInfo);
        pendingInputEvents = getRuntimeContext().getListState(pendingInputEventsDescriptor);

        processingTimeService.registerTimer(
                processingTimeService.getCurrentProcessingTime() + 3000,
                (long timestamp) -> {
                    processPendingInputEventsAndRegisterTimer();
                });
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    private boolean canExecuteInputEvent(K key) {
        try {
            Integer pendingActionCount = key2pendingActionCount.get(key);
            if (pendingActionCount == null) {
                return true;
            }
            return pendingActionCount == 0;
        } catch (Exception e) {
            return true;
        }
    }

    private void processPendingInputEventsAndRegisterTimer() {
        List<DataMessage<K>> continuePendingInputEvents = new ArrayList<>();
        try {
            Iterator<DataMessage<K>> iterator = pendingInputEvents.get().iterator();
            while (iterator.hasNext()) {
                DataMessage<K> dataMessage = iterator.next();
                if (canExecuteInputEvent(dataMessage.getKey())) {
                    List<Action> pendingActions =
                            workflowPlan.getAction(dataMessage.getEventType());
                    int delta = pendingActions == null ? 0 : pendingActions.size();
                    addPendingActionCount(dataMessage.getKey(), delta);
                    mailboxExecutor.submit(
                            () -> this.processElement(new StreamRecord<>(dataMessage)),
                            "process pending input event");
                } else {
                    continuePendingInputEvents.add(dataMessage);
                }
            }
            pendingInputEvents.update(continuePendingInputEvents);

            processingTimeService.registerTimer(
                    processingTimeService.getCurrentProcessingTime() + 3000,
                    (long timestamp) -> {
                        processPendingInputEventsAndRegisterTimer();
                    });
        } catch (Exception e) {

        }
    }

    private void addPendingActionCount(K key, int delta) {
        if (delta == 0) {
            return;
        }
        try {
            Integer pendingActionCount = key2pendingActionCount.get(key);
            if (pendingActionCount == null) {
                pendingActionCount = 0;
            }
            pendingActionCount += delta;
            if (pendingActionCount == 0) {
                key2pendingActionCount.remove(key);
            } else {
                key2pendingActionCount.put(key, pendingActionCount);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    void initInterpreter() {
        PythonInterpreterConfig config =
                PythonInterpreterConfig.newBuilder()
                        .setPythonExec(
                                "/Users/huangxu/Downloads/flink-agents-poc-python/pythonProject/.venv/bin/python") // specify python exec, use "python" on Windows
                        .addPythonPaths(
                                "/Users/huangxu/Desktop/codingspace/flink-agents/python:/Users/huangxu/Downloads/flink-agents-poc-python/pythonProject/.venv/lib/python3.10/site-packages")
                        .build();
        this.interpreter = new PythonInterpreter(config);
    }

    private DataMessage<K> executePythonFunction(
            PythonFunction pythonFunction, DataMessage<K> inputMessage)
            throws JsonProcessingException {
        int lastIndexOf = pythonFunction.getModule().lastIndexOf(".");
        String s1 = pythonFunction.getModule().substring(0, lastIndexOf);
        String s2 = pythonFunction.getModule().substring(lastIndexOf + 1);
        interpreter.exec("from " + s1 + " import " + s2);
        Object invoked =
                interpreter.invoke(pythonFunction.getQualName(), inputMessage.getPayload());

        ObjectMapper mapper = new ObjectMapper();

        PythonFunctionInvokeResult invokeResult =
                mapper.readValue(invoked.toString(), PythonFunctionInvokeResult.class);
        return new PythonDataMessage<>(
                invokeResult.eventType, inputMessage.getKey(), invokeResult.payload.getBytes());
    }

    private static class PythonFunctionInvokeResult {
        private String eventType;
        private String payload;

        public PythonFunctionInvokeResult() {}

        public PythonFunctionInvokeResult(String eventType, String payload) {
            this.eventType = eventType;
            this.payload = payload;
        }

        public String getEventType() {
            return eventType;
        }

        public void setEventType(String eventType) {
            this.eventType = eventType;
        }

        public String getPayload() {
            return payload;
        }

        public void setPayload(String payload) {
            this.payload = payload;
        }
    }
}
