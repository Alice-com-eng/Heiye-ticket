package org.opengoofy.index12306.biz.ticketservice.common.exception;

/**
 * 座位区间冲突，可重试换座
 */
public class SeatOccupiedRetryException extends RuntimeException {

    public SeatOccupiedRetryException(String message) {
        super(message);
    }
}