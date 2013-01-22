/*
 * Copyright 2012 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.queues;

import java.util.Queue;

public class QueuePerfTest
{
    public static final int QUEUE_CAPACITY = 32 * 1024;
    public static final int REPETITIONS = 50 * 1000 * 1000;
    public static final Integer TEST_VALUE = Integer.valueOf(777);

    public static void main(final String[] args) throws Exception
    {
        final Queue<Integer> queue = createQueue(args[0]);

        for (int i = 0; i < 5; i++)
        {
            System.gc();
            performanceRun(i, queue);
        }
    }

    private static Queue<Integer> createQueue(final String option)
    {
        switch (Integer.parseInt(option))
        {
            case 1: return new OneToOneConcurrentArrayQueue<Integer>(QUEUE_CAPACITY);
            case 2: return new OneToOneConcurrentArrayQueue2<Integer>(QUEUE_CAPACITY);
            case 3: return new OneToOneConcurrentArrayQueue3<Integer>(QUEUE_CAPACITY);
            case 4: return new java.util.concurrent.ArrayBlockingQueue<Integer>(QUEUE_CAPACITY);
            case 5: return new java.util.concurrent.LinkedBlockingQueue<Integer>(QUEUE_CAPACITY);
            case 6: return new java.util.concurrent.ConcurrentLinkedQueue<Integer>();
//            case 7: return new java.util.concurrent.LinkedTransferQueue<Integer>();

            default: throw new IllegalArgumentException("Invalid option: " + option);
        }
    }

    private static void performanceRun(final int runNumber, final Queue<Integer> queue) throws Exception
    {
        final long start = System.nanoTime();
        final Thread thread = new Thread(new Producer(queue));
        thread.start();

        Integer result;
        int i = REPETITIONS;
        do
        {
            while (null == (result = queue.poll()))
            {
                Thread.yield();
            }
        }
        while (0 != --i);

        thread.join();

        final long duration = System.nanoTime() - start;
        final long ops = (REPETITIONS * 1000L * 1000L * 1000L) / duration;
        System.out.format("%d - ops/sec=%,d - %s result=%d\n",
                          Integer.valueOf(runNumber), Long.valueOf(ops),
                          queue.getClass().getSimpleName(), result);
    }

    public static class Producer implements Runnable
    {
        private final Queue<Integer> queue;

        public Producer(final Queue<Integer> queue)
        {
            this.queue = queue;
        }

        public void run()
        {
            int i = REPETITIONS;
            do
            {
                while (!queue.offer(TEST_VALUE))
                {
                    Thread.yield();
                }
            }
            while (0 != --i);
        }
    }
}
