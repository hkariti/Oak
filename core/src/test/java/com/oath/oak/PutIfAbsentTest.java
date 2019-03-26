package com.oath.oak;

import org.junit.Test;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


public class PutIfAbsentTest {



    @Test(timeout=10_000)
    public void testConcurrentPutOrCompute() {
        OakMapBuilder<Integer, Integer> builder = OakMapBuilder.getDefaultBuilder();
        OakMap<Integer, Integer> oak = builder.build();

        CountDownLatch startSignal = new CountDownLatch(1);

        ExecutorService executor = Executors.newFixedThreadPool(16);

        List<Future<Integer>> threads = new ArrayList<>();
        Integer numThreads = 16;
        int numKeys = 1000000;

        for (int i = 0; i < numThreads; ++i ) {
            Callable<Integer> operation = () -> {
                int counter = 0;
                try {
                    startSignal.await();

                    for (int j = 0; j < numKeys; ++j) {
                        oak.putIfAbsentComputeIfPresent(j, 1, buffer -> {
                            int currentVal = buffer.getInt(0);
                            buffer.putInt(0, currentVal + 1);
                        });
//                        if (retval) counter++;
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return counter;
            };

            Future<Integer> future = executor.submit(operation);
            threads.add(future);
        }

        startSignal.countDown();
        final int[] returnValues = {0};
        threads.forEach(t -> {
            try {
                returnValues[0] += t.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                fail();
            }
        });

        OakIterator<Integer> iterator = oak.valuesIterator();
        int count2 = 0;
        while(iterator.hasNext()) {
            Integer value = iterator.next();
            assertEquals(numThreads, value);
            count2++;
        }
        assertEquals(count2, numKeys);
        assertEquals(numKeys, returnValues[0]);
    }
}
