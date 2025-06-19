package org.apache.flink.agents.runtime.operator;

import org.apache.flink.agents.runtime.common.MailboxExecutorFacade;
import org.apache.flink.agents.runtime.feedback.Checkpoints;
import org.apache.flink.agents.runtime.feedback.FeedbackChannel;
import org.apache.flink.agents.runtime.feedback.FeedbackChannelBroker;
import org.apache.flink.agents.runtime.feedback.FeedbackConsumer;
import org.apache.flink.agents.runtime.feedback.FeedbackKey;
import org.apache.flink.agents.runtime.feedback.SubtaskFeedbackKey;
import org.apache.flink.agents.runtime.logger.Loggers;
import org.apache.flink.agents.runtime.logger.UnboundedFeedbackLogger;
import org.apache.flink.agents.runtime.logger.UnboundedFeedbackLoggerFactory;
import org.apache.flink.agents.runtime.message.DataMessage;
import org.apache.flink.agents.runtime.message.Message;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.state.KeyGroupStatePartitionStreamProvider;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.function.SerializableFunction;

import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.Executor;

/** h. */
public class FeedbackOperator<K> extends AbstractStreamOperator<DataMessage<K>>
        implements FeedbackConsumer<DataMessage<K>>,
                OneInputStreamOperator<DataMessage<K>, DataMessage<K>> {
    private static final long serialVersionUID = 1L;
    private final TypeSerializer<DataMessage<K>> elementSerializer;
    private final FeedbackKey<DataMessage<K>> feedbackKey;
    private final MailboxExecutor mailboxExecutor;
    private final SerializableFunction<DataMessage<K>, K> keySelector;
    private final long totalMemoryUsedForFeedbackCheckpointing;
    private transient StreamRecord<DataMessage<K>> reusable;
    private transient Checkpoints<DataMessage<K>> checkpoints;

    private transient boolean closedOrDisposed;

    FeedbackOperator(
            FeedbackKey<DataMessage<K>> feedbackKey,
            SerializableFunction<DataMessage<K>, K> keySelector,
            long totalMemoryUsedForFeedbackCheckpointing,
            TypeSerializer<DataMessage<K>> elementSerializer,
            MailboxExecutor mailboxExecutor,
            ProcessingTimeService processingTimeService) {
        this.feedbackKey = Objects.requireNonNull(feedbackKey);
        this.keySelector = Objects.requireNonNull(keySelector);
        this.totalMemoryUsedForFeedbackCheckpointing = totalMemoryUsedForFeedbackCheckpointing;
        this.elementSerializer = elementSerializer;
        this.mailboxExecutor = mailboxExecutor;
        this.processingTimeService = processingTimeService;
    }

    @Override
    public void processFeedback(DataMessage<K> element) throws Exception {
        if (closedOrDisposed) {
            return;
        }
        System.out.println("FeedbackOperator processFeedback " + element);
        OptionalLong maybeCheckpoint = element.isBarrierMessage();
        if (maybeCheckpoint.isPresent()) {
            checkpoints.commitCheckpointsUntil(maybeCheckpoint.getAsLong());
        } else {
            sendDownstream(element);
            checkpoints.append(element);
        }
    }

    @Override
    public void processElement(StreamRecord<DataMessage<K>> streamRecord) throws Exception {
        System.out.println("FeedbackOperator processElement " + streamRecord);
        sendDownstream(streamRecord.getValue());
    }

    @Override
    @SuppressWarnings("unchecked")
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        final IOManager ioManager = getContainingTask().getEnvironment().getIOManager();
        final int maxParallelism =
                getRuntimeContext().getTaskInfo().getMaxNumberOfParallelSubtasks();

        this.reusable = new StreamRecord<>(null);

        //
        // Initialize the unbounded feedback logger
        //
        UnboundedFeedbackLoggerFactory<DataMessage<K>> feedbackLoggerFactory =
                (UnboundedFeedbackLoggerFactory<DataMessage<K>>)
                        Loggers.unboundedSpillableLoggerFactory(
                                ioManager,
                                maxParallelism,
                                totalMemoryUsedForFeedbackCheckpointing,
                                elementSerializer,
                                keySelector);

        this.checkpoints = new Checkpoints<>(feedbackLoggerFactory::create);

        //
        // we first must reply previously check-pointed envelopes before we start
        // processing any new envelopes.
        //
        UnboundedFeedbackLogger<DataMessage<K>> logger = feedbackLoggerFactory.create();
        for (KeyGroupStatePartitionStreamProvider keyedStateInput :
                context.getRawKeyedStateInputs()) {
            logger.replyLoggedEnvelops(keyedStateInput.getStream(), this);
        }

        registerFeedbackConsumer(new MailboxExecutorFacade(mailboxExecutor, "Feedback Consumer"));
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        checkpoints.startLogging(
                context.getCheckpointId(), context.getRawKeyedOperatorStateOutput());
    }

    @Override
    protected boolean isUsingCustomRawKeyedState() {
        return true;
    }

    @Override
    public void close() throws Exception {
        closeInternally();
        super.close();
    }

    private void closeInternally() {
        IOUtils.closeQuietly(checkpoints);
        checkpoints = null;
        closedOrDisposed = true;
    }

    private void registerFeedbackConsumer(Executor mailboxExecutor) {
        final int indexOfThisSubtask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        final int attemptNum = getRuntimeContext().getTaskInfo().getAttemptNumber();
        final SubtaskFeedbackKey<DataMessage<K>> key =
                feedbackKey.withSubTaskIndex(indexOfThisSubtask, attemptNum);
        FeedbackChannelBroker broker = FeedbackChannelBroker.get();
        FeedbackChannel<DataMessage<K>> channel = broker.getChannel(key);
        channel.registerConsumer(this, mailboxExecutor);
    }

    private void sendDownstream(Message element) {
        reusable.replace(element);
        output.collect(reusable);
    }
}
