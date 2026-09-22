/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opengoofy.index12306.biz.ticketservice.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.opengoofy.index12306.biz.orderservice.dao.entity.OrderDO;
import org.opengoofy.index12306.biz.ticketservice.dao.entity.StationDO;
import org.opengoofy.index12306.biz.ticketservice.dto.req.CancelTicketOrderReqDTO;
import org.opengoofy.index12306.biz.ticketservice.dto.req.PurchaseTicketReqDTO;
import org.opengoofy.index12306.biz.ticketservice.dto.req.RefundTicketReqDTO;
import org.opengoofy.index12306.biz.ticketservice.dto.req.TicketPageQueryReqDTO;
import org.opengoofy.index12306.biz.ticketservice.dto.resp.RefundTicketRespDTO;
import org.opengoofy.index12306.biz.ticketservice.dto.resp.TicketPageQueryRespDTO;
import org.opengoofy.index12306.biz.ticketservice.dto.resp.TicketPurchaseRespDTO;
import org.opengoofy.index12306.biz.ticketservice.remote.dto.PayInfoRespDTO;
import org.opengoofy.index12306.biz.ticketservice.service.TicketService;
import org.opengoofy.index12306.framework.starter.convention.page.PageResponse;
import org.opengoofy.index12306.framework.starter.convention.result.Result;
import org.opengoofy.index12306.framework.starter.web.Results;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * 车票控制层
  
 */
@RestController
@RequiredArgsConstructor
@Tag(name = "车票核心业务接口")
public class TicketController {

    private final TicketService ticketService;

    /**
     * 根据条件查询车票
     */
    @PostMapping("/api/ticket-service/ticket/query")
    @Operation(summary = "根据条件查询车票")
    public Result<TicketPageQueryRespDTO> pageListTicketQuery(@RequestBody TicketPageQueryReqDTO requestParam) {
        return Results.success(ticketService.pageListTicketQueryV1(requestParam));
    }

    /**
     * 购买车票
     */
    @PostMapping("/api/ticket-service/ticket/purchase/v2")
    @Operation(summary = "购买车票")
    public Result<TicketPurchaseRespDTO> purchaseTicketsV2(@RequestBody PurchaseTicketReqDTO requestParam) {
        return Results.success(ticketService.purchaseTicketsV2(requestParam));
    }


    /**
     * 取消车票订单
     */
    @PostMapping("/api/ticket-service/ticket/cancel")
    @Operation(summary = "取消车票订单")
    public Result<Void> cancelTicketOrder(@RequestBody CancelTicketOrderReqDTO requestParam) {
        ticketService.cancelTicketOrder(requestParam);
        return Results.success();
    }

    /**
     * 支付单详情查询
     */
    @GetMapping("/api/ticket-service/ticket/pay")
    @Operation(summary = "用户支付")
    public Result<Void> pay(@RequestParam("orderSn") String orderSn) {
        ticketService.payOrder(orderSn);
        return Results.success();}

    /**
     * 公共退款接口
     */
    @PostMapping("/api/ticket-service/ticket/refund")
    @Operation(summary = "用户退款")
    public Result<RefundTicketRespDTO> commonTicketRefund(@RequestBody RefundTicketReqDTO requestParam) {
        return Results.success(ticketService.commonTicketRefund(requestParam));
    }
    /**
     * 分页查询订单
     */
    @GetMapping("/api/ticket-service/ticket/order/page")
    @Operation(summary = "分页查询订单")
    public Result<PageResponse<OrderDO>> pageOrder(@RequestParam("pageNum") Integer pageNum,
                                                   @RequestParam("pageSize") Integer pageSize) {
        return Results.success(ticketService.pageOrder(pageNum, pageSize));
    }
    /**
     * 查询所有车站
     */
    @GetMapping("/api/ticket-service/station/query")
    @Operation(summary = "查询所有车站")
    public Result<List<StationDO>> listStation() {
        return Results.success(ticketService.listStation());
    }
    /**
     * 根据订单号查询订单
     */
    @GetMapping("/api/ticket-service/ticket/order/query")
    @Operation(summary = "根据订单号查询订单")
    public Result<OrderDO> queryOrderByOrderSn(@RequestParam("orderSn") String orderSn) {
        return Results.success(ticketService.queryOrderByOrderSn(orderSn));
    }
}