package org.opengoofy.index12306.biz.ticketservice.service.handler.ticket.filter.query;

import lombok.RequiredArgsConstructor;
import org.opengoofy.index12306.biz.ticketservice.dto.req.TicketPageQueryReqDTO;
import org.opengoofy.index12306.framework.starter.convention.exception.ClientException;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Objects;

@Component
@RequiredArgsConstructor
public class TrainTicketQueryParamBaseVerifyChainFilter implements TrainTicketQueryChainFilter<TicketPageQueryReqDTO> {

    @Override
    public void handler(TicketPageQueryReqDTO requestParam) {
        // 日期不为空时才校验
        if (requestParam.getDepartureDate() != null
                && requestParam.getDepartureDate().toInstant().atZone(ZoneId.systemDefault()).toLocalDate().isBefore(LocalDate.now())) {
            throw new ClientException("出发日期不能小于当前日期");
        }
        if (Objects.equals(requestParam.getFromStation(), requestParam.getToStation())) {
            throw new ClientException("出发地和目的地不能相同");
        }
    }

    @Override
    public int getOrder() {
        return 10;
    }
}