package dev.philippelachaud.processor.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.reactive.TransactionalOperator;

@Configuration
@EnableTransactionManagement
public class TransactionConfig {

    /**
     * Creates a TransactionalOperator bean for managing transactions, in a reactive manner.
     *
     * @param transactionManager The ReactiveTransactionManager to use for transaction management.
     * @return The TransactionalOperator bean.
     */
    @Bean
    public TransactionalOperator transactionalOperator(
            @Qualifier("connectionFactoryTransactionManager") ReactiveTransactionManager transactionManager) {
        return TransactionalOperator.create(transactionManager);
    }
}
