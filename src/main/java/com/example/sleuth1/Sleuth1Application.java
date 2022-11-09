package com.example.sleuth1;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.cloud.sleuth.Tracer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.util.UriComponentsBuilder;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

@SpringBootApplication
public class Sleuth1Application {

    public static void main(String[] args) {
        SpringApplication.run(Sleuth1Application.class, args);
    }

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplateBuilder().build();
    }

}

@RequiredArgsConstructor
@RestController
class TestController {

    private final Sleuth2Adapter sleuth2Adapter;

    @PostMapping("/api/test")
    public ResponseEntity<Void> test(@RequestBody SimpleMessageResource resource) {

        sleuth2Adapter.callSleuth2(resource.getMessage());

        return ResponseEntity.noContent().build();
    }

}

@Slf4j
@Component
@RequiredArgsConstructor
class Sleuth2Adapter {

    private final RestTemplate restTemplate;

    public void callSleuth2(String message) {

        log.info("Calling service sleuth-2");

        URI url = UriComponentsBuilder.fromHttpUrl("http://localhost:8082/api/test")
                .queryParam("message", message)
                .build()
                .toUri();

        ResponseEntity<String> response = restTemplate.getForEntity(url, String.class);

        String result = response.getBody();

        log.info("Received response {}", result);

    }
}

@RequiredArgsConstructor
@Component
class ResponseHeaderFilter extends OncePerRequestFilter {

    private final Tracer tracer;

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {

        response.addHeader("X-Correlation-Id", tracer.currentSpan().context().traceId());
        filterChain.doFilter(request, response);
    }

}


@Slf4j
@RequiredArgsConstructor
@Service
class SimpleMessageService {

    private Random random = new Random();
    private final SimpleMessageRepository simpleMessageRepository;
    //private final Tracer tracer;

    @KafkaListener(groupId = "service-a", topics = "simple", containerFactory = "simpleMessageKafkaListenerContainerFactory")
    public void receive(@Payload SimpleMessage consumerMessage, @Headers MessageHeaders messageHeaders,
                        @Header(KafkaHeaders.RECEIVED_TIMESTAMP) Long timestamp, @Header(KafkaHeaders.OFFSET) Long offset,
                        @Header(KafkaHeaders.RECEIVED_PARTITION_ID) Integer partitionId) {

        log.info("Received message from kafka queue: {} with offset {} and partition {}", consumerMessage, offset, partitionId);

        messageHeaders.forEach((s, o) -> log.info(s + ": " + o));

        /*
        if (!random.nextBoolean()) {
            throw new RuntimeException("Random Error");
        }
         */

        LocalDateTime receivedAt = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.of("UTC"));

        SimpleMessageEntity entity = new SimpleMessageEntity();
        entity.setMessage(consumerMessage.getMessage());
        entity.setKafkaOffset(offset);
        entity.setPartitionId(partitionId);
        entity.setTimestamp(receivedAt);
        //entity.setTraceId(tracer.currentSpan().context().traceId());
        simpleMessageRepository.save(entity);

    }

}

@Configuration
class KafkaConfiguration {

    private String bootstrapAddress = "localhost:9093";

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, SimpleMessage> simpleMessageKafkaListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<String, SimpleMessage> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(simpleMessageConsumerFactory());
//		factory.setErrorHandler((exception,data) -> {
//			log.error("Error in process with Exception {} and the record is {}", exception, data);
//		});
        return factory;
    }

    @Bean
    public ConsumerFactory<String, SimpleMessage> simpleMessageConsumerFactory() {

        return new DefaultKafkaConsumerFactory<>(
                getConsumerProps(),
                new StringDeserializer(),
                new CustomDeserializer<>(SimpleMessage.class));
    }

    private Map<String, Object> getConsumerProps() {
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CustomDeserializer.class);
        return consumerProps;
    }

}

class CustomDeserializer<T> implements Deserializer<T> {

    private final ObjectMapper objectMapper;
    final Class<T> typeParameterClass;

    public CustomDeserializer(Class<T> typeParameterClass) {
        this.typeParameterClass = typeParameterClass;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    @SneakyThrows
    @Override
    public T deserialize(String topic, byte[] data) {
        return objectMapper.readValue(data, typeParameterClass);
    }
}

@Repository
interface SimpleMessageRepository extends JpaRepository<SimpleMessageEntity, Long> {}

@Slf4j
@Aspect
@Component
class RestControllerLoggingAspect {

    @Pointcut("within(@org.springframework.web.bind.annotation.RestController *)")
    public void controllerInvocation() {}

    @Around("controllerInvocation()")
    public Object measureMethodExecutionTime(ProceedingJoinPoint pjp) throws Throwable {
        log.info("Rest Controller Invocation (ENTER) on method " + pjp.getSignature().getName());
        Object result = pjp.proceed();
        log.info("Rest Controller Invocation (EXIT) on method " + pjp.getSignature().getName());
        return result;
    }

}

@Data
class SimpleMessage {

    private String message;

}

@Data
class SimpleMessageResource {

    String message;

}

@NoArgsConstructor
@Data
@Entity
class SimpleMessageEntity {

    @Id
    @GeneratedValue
    private long id;

    @Column
    private String message;

    @Column
    private LocalDateTime timestamp;

    @Column
    private Integer partitionId;

    @Column
    private Long kafkaOffset;

    @Column
    private String traceId;

}

