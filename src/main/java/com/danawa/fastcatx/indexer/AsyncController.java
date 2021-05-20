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
public class AsyncController {
    private static Logger logger = LoggerFactory.getLogger(AsyncController.class);

    private enum ACTION { FULL_INDEX, DYNAMIC_INDEX }
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
        Job job = indexJobManager.start(ACTION.FULL_INDEX.name(), payload);
        return new ResponseEntity<>(job, HttpStatus.OK);
    }

    @PostMapping(value = "/dynamic")
    public ResponseEntity<?> doDynamic(@RequestBody Map<String, Object> payload) {
        Job job = indexJobManager.start(ACTION.DYNAMIC_INDEX.name(), payload);
        return new ResponseEntity<>(job, HttpStatus.OK);
    }

    @DeleteMapping(value = "/{id}")
    public ResponseEntity<?> remove(@PathVariable UUID id) {
        return new ResponseEntity<>(indexJobManager.remove(id), HttpStatus.OK);
    }

    @PutMapping(value = "/{id}/sub_start")
    public ResponseEntity<?> subStart(@PathVariable UUID id,
                                      @RequestParam String groupSeq) {
        Job job = indexJobManager.status(id);
        job.getGroupSeq().add(Integer.parseInt(groupSeq));
        return new ResponseEntity<>(job, HttpStatus.OK);
    }

    @GetMapping
    public ResponseEntity<?> ids() {
        return new ResponseEntity<>(indexJobManager.getIds(), HttpStatus.OK);
    }

}

