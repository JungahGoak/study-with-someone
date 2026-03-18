package com.koa.sws.controller;

import com.koa.sws.service.SessionService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequiredArgsConstructor
public class ViewerController {

    private final SessionService sessionService;

    @GetMapping("/viewers")
    public Map<String, Integer> getViewerCount() {
        return Map.of("count", sessionService.getSessionCount());
    }
}