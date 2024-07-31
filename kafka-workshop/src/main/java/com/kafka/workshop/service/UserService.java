package com.kafka.workshop.service;

import com.kafka.workshop.confidg.kafka.properties.UserCreatedTopicProperties;
import com.kafka.workshop.dto.UserCreateRequest;
import com.kafka.workshop.dto.UserCreatedPayload;
import com.kafka.workshop.entity.User;
import com.kafka.workshop.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.springframework.kafka.support.KafkaHeaders.MESSAGE_KEY;
import static org.springframework.kafka.support.KafkaHeaders.TOPIC;

@Service
@RequiredArgsConstructor
public class UserService {

    // RequiredArgsConstructor final tipleri constructora otomatik ekliyor inject etmis oluyor.
    private final UserRepository userRepository;

    private final com.example.userservice.config.kafka.producer.KafkaProducer kafkaProducer;

    private final UserCreatedTopicProperties userCreatedTopicProperties;

    public User createUser(UserCreateRequest userCreateRequest) {
        User user = User.getUser(userCreateRequest);
        User savedUser = userRepository.save(user);

        UserCreatedPayload payload = UserCreatedPayload.getUserCreatedPayload(savedUser,
                userCreateRequest.getAddressText());

        // send it to another project with kafka here!

        Map<String, Object> headers = new HashMap<>();
        headers.put(TOPIC, userCreatedTopicProperties.getTopicName());
        headers.put(MESSAGE_KEY, savedUser.getId().toString());

        kafkaProducer.sendMessage(new GenericMessage<>(payload, headers));
        return savedUser;
    }

    public User getUserById(Long id) {
        Optional<User> userOptional = userRepository.findById(id);
        if (!userOptional.isPresent()) {
            return null;
        }
        return userOptional.get();
    }

}
