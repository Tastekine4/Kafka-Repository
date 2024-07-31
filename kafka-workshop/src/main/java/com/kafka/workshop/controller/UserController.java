package com.kafka.workshop.controller;

import com.kafka.workshop.dto.AddressResponseDto;
import com.kafka.workshop.dto.UserCreateRequest;
import com.kafka.workshop.dto.UserResponse;
import com.kafka.workshop.entity.User;
import com.kafka.workshop.service.UserService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

@RestController
@RequestMapping("/api/user")
@RequiredArgsConstructor
public class UserController {

    private RestTemplate restTemplate = new RestTemplate();

    private final UserService userService;

    @PostMapping
    public User create(@RequestBody UserCreateRequest userCreateRequest) {
        return userService.createUser(userCreateRequest);
    }

    @GetMapping("/{userId}")
    public UserResponse getUserAddress(@PathVariable Long userId) {
        // You have to change user-address service port according to running port
        String url = String.format("http://localhost:8002/api/address/%s", userId);
        ResponseEntity<AddressResponseDto> address =
                restTemplate.getForEntity(url, AddressResponseDto.class);

        User user = userService.getUserById(address.getBody().getUserId());
        return UserResponse.getUserResponseWithAddress(user, address.getBody());
    }
}
