package io.yorkecao.sparkclientdemo.web;

import io.yorkecao.sparkclientdemo.service.SparkClientService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;

/**
 * @author Yorke
 */
@Slf4j
@RestController
@RequestMapping("sparkClient")
public class SparkClientController {

    private SparkClientService sparkClientService;

    public SparkClientController(SparkClientService sparkClientService) {
        this.sparkClientService = sparkClientService;
    }

    @PostMapping("submit")
    public ResponseEntity<?> submit(@RequestBody String[] params) {
        try {
            System.out.println(Arrays.toString(params));
            this.sparkClientService.submit(params);
            return ResponseEntity.ok("success");
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return ResponseEntity.badRequest().body(e.getMessage());
        }
    }
}
