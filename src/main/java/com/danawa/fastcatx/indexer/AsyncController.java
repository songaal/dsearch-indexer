package com.danawa.fastcatx.indexer;

import com.danawa.fastcatx.indexer.entity.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/async")
@CrossOrigin(value = "*", methods = {RequestMethod.DELETE, RequestMethod.GET, RequestMethod.DELETE, RequestMethod.PUT, RequestMethod.POST})
public class AsyncController {
    private static Logger logger = LoggerFactory.getLogger(AsyncController.class);

    private final IndexJobManager indexJobManager;

    public AsyncController(IndexJobManager indexJobManager) {
        this.indexJobManager = indexJobManager;
    }

    @PutMapping(value = "/stop")
    public ResponseEntity<?> stop(@RequestParam String id) {
        Job job = indexJobManager.stop(UUID.fromString(id));
        return new ResponseEntity<>(job, HttpStatus.OK);
    }

    @GetMapping(value = "/status")
    public ResponseEntity<?> getStatus(@RequestParam String id) {
        Job job = indexJobManager.status(UUID.fromString(id));
        return new ResponseEntity<>(job, HttpStatus.OK);
    }

    @PostMapping(value = "/start")
    public ResponseEntity<?> doStart(@RequestBody Map<String, Object> payload) {
        Job job = indexJobManager.start(payload);
        return new ResponseEntity<>(job, HttpStatus.OK);
    }

}

