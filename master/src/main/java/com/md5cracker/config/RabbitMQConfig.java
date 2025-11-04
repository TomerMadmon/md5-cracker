package com.md5cracker.config;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQConfig {
    
    public static final String EXCHANGE_NAME = "md5.exchange";
    public static final String LOOKUP_QUEUE = "md5.lookup";
    public static final String RESULTS_QUEUE = "md5.results";
    public static final String LOOKUP_ROUTING_KEY = "md5.lookup";
    public static final String RESULTS_ROUTING_KEY = "md5.results";

    @Bean
    public TopicExchange md5Exchange() {
        return new TopicExchange(EXCHANGE_NAME);
    }

    @Bean
    public Queue lookupQueue() {
        return QueueBuilder.durable(LOOKUP_QUEUE).build();
    }

    @Bean
    public Queue resultsQueue() {
        return QueueBuilder.durable(RESULTS_QUEUE).build();
    }

    @Bean
    public Binding lookupBinding() {
        return BindingBuilder.bind(lookupQueue())
                .to(md5Exchange())
                .with(LOOKUP_ROUTING_KEY);
    }

    @Bean
    public Binding resultsBinding() {
        return BindingBuilder.bind(resultsQueue())
                .to(md5Exchange())
                .with(RESULTS_ROUTING_KEY);
    }

    @Bean
    public Jackson2JsonMessageConverter messageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(messageConverter());
        return template;
    }
}

