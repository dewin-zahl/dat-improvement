package ai.dat.core.agent;

import ai.dat.core.adapter.DatabaseAdapter;
import ai.dat.core.agent.data.StreamAction;
import ai.dat.core.agent.data.StreamEvent;
import ai.dat.core.contentstore.ContentStore;
import ai.dat.core.contentstore.data.QuestionSqlPair;
import ai.dat.core.semantic.data.SemanticModel;
import ai.dat.core.utils.JinjaTemplateUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static ai.dat.core.agent.DefaultEventOptions.*;

/**
 * @Author JunjieM
 * @Date 2025/6/25
 */
@Slf4j
public abstract class AbstractAskdataAgent implements AskdataAgent {

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private static final ExecutorService executor;

    static {
        int corePoolSize = 4;
        int maxPoolSize = Math.min(Runtime.getRuntime().availableProcessors() * 4, 32); // 上限32
        long keepAliveTime = 30L;
        int queueCapacity = 1000;

        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("dat-agent-%d")
                .setDaemon(true)  // 关键：设置为守护线程，允许JVM退出
                .build();

        executor = new ThreadPoolExecutor(
                corePoolSize,
                maxPoolSize,
                keepAliveTime,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(queueCapacity),  // 有界队列
                threadFactory,
                new ThreadPoolExecutor.CallerRunsPolicy()  // 拒绝策略：调用者执行
        );

        // 注册JVM关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down DAT agent executor service...");
            executor.shutdown();
            try {
                // 等待现有任务完成
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    log.warn("DAT agent executor did not terminate within 60 seconds");
                    // 取消当前执行的任务
                    executor.shutdownNow();
                    // 等待任务响应取消
                    if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                        log.error("DAT agent executor did not terminate");
                    }
                }
            } catch (InterruptedException ie) {
                // 当前线程被中断，重新中断状态
                Thread.currentThread().interrupt();
                // 取消任务
                executor.shutdownNow();
            }
            log.info("DAT agent executor service shut down complete");
        }, "dat-agent-shutdown-hook"));
    }

    protected final StreamAction action = new StreamAction();

    protected final ContentStore contentStore;
    protected final DatabaseAdapter databaseAdapter;
    protected final Map<String, Object> variables;

    public AbstractAskdataAgent(@NonNull ContentStore contentStore,
                                @NonNull DatabaseAdapter databaseAdapter,
                                Map<String, Object> variables) {
        this.contentStore = contentStore;
        this.databaseAdapter = databaseAdapter;
        this.variables = Optional.ofNullable(variables).orElse(Collections.emptyMap());
    }

    @Deprecated
    public AbstractAskdataAgent(@NonNull ContentStore contentStore,
                                @NonNull DatabaseAdapter databaseAdapter) {
        this(contentStore, databaseAdapter, null);
    }

    @Override
    public ContentStore contentStore() {
        return contentStore;
    }

    @Override
    public StreamAction ask(@NonNull String question) {
        return ask(question, Collections.emptyList());
    }

    public StreamAction ask(@NonNull String question, @NonNull List<QuestionSqlPair> histories) {
        action.start();
        executor.execute(() -> {
            try {
                run(question, histories);
            } catch (Exception e) {
                log.error("Ask data exception", e);
                action.add(StreamEvent.from(EXCEPTION_EVENT, MESSAGE, e.getMessage()));
            } finally {
                action.finished();
            }
        });
        return action;
    }

    protected abstract void run(String question, List<QuestionSqlPair> histories);

    protected List<Map<String, Object>> executeQuery(@NonNull String semanticSql,
                                                     @NonNull List<SemanticModel> semanticModels) throws SQLException {
        List<SemanticModel> renderedSemanticModels = semanticModels.stream().map(m -> {
            try {
                SemanticModel semanticModel = JSON_MAPPER.readValue(
                        JSON_MAPPER.writeValueAsString(m), SemanticModel.class);
                semanticModel.setModel(JinjaTemplateUtil.render(semanticModel.getModel(), variables));
                return semanticModel;
            } catch (JsonProcessingException ex) {
                throw new RuntimeException(ex);
            }
        }).collect(Collectors.toList());
        String sql;
        try {
            sql = databaseAdapter.generateSql(semanticSql, renderedSemanticModels);
            log.info("dialectSql: " + sql);
            action.add(StreamEvent.from(SEMANTIC_TO_SQL_EVENT, SQL, sql));
        } catch (Exception e) {
            action.add(StreamEvent.from(SEMANTIC_TO_SQL_EVENT, ERROR, e.getMessage()));
            throw new RuntimeException(e);
        }
        try {
            List<Map<String, Object>> results = databaseAdapter.executeQuery(sql);
            action.add(StreamEvent.from(SQL_EXECUTE_EVENT, DATA, results));
            return results;
        } catch (SQLException e) {
            action.add(StreamEvent.from(SQL_EXECUTE_EVENT, ERROR, e.getMessage()));
            throw new SQLException(e);
        }
    }
}
