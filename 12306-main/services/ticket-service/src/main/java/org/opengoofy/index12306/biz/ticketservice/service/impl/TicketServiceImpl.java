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

package org.opengoofy.index12306.biz.ticketservice.service.impl;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson2.JSON;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.Lists;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opengoofy.index12306.biz.orderservice.common.enums.OrderStatusEnum;
import org.opengoofy.index12306.biz.orderservice.dao.entity.OrderDO;
import org.opengoofy.index12306.biz.orderservice.dao.mapper.OrderMapper;
import org.opengoofy.index12306.biz.ticketservice.common.enums.TicketChainMarkEnum;
import org.opengoofy.index12306.biz.ticketservice.common.enums.TicketStatusEnum;
import org.opengoofy.index12306.biz.ticketservice.common.enums.VehicleTypeEnum;
import org.opengoofy.index12306.biz.ticketservice.common.exception.SeatOccupiedRetryException;
import org.opengoofy.index12306.biz.ticketservice.dao.entity.*;
import org.opengoofy.index12306.biz.ticketservice.dao.mapper.StationMapper;
import org.opengoofy.index12306.biz.ticketservice.dao.mapper.TicketMapper;
import org.opengoofy.index12306.biz.ticketservice.dao.mapper.TrainMapper;
import org.opengoofy.index12306.biz.ticketservice.dao.mapper.TrainStationPriceMapper;
import org.opengoofy.index12306.biz.ticketservice.dao.mapper.TrainStationRelationMapper;
import org.opengoofy.index12306.biz.ticketservice.dto.domain.PurchaseTicketPassengerDetailDTO;
import org.opengoofy.index12306.biz.ticketservice.dto.domain.SeatClassDTO;
import org.opengoofy.index12306.biz.ticketservice.dto.domain.SeatTypeCountDTO;
import org.opengoofy.index12306.biz.ticketservice.dto.domain.TicketListDTO;
import org.opengoofy.index12306.biz.ticketservice.dto.req.CancelTicketOrderReqDTO;
import org.opengoofy.index12306.biz.ticketservice.dto.req.PurchaseTicketReqDTO;
import org.opengoofy.index12306.biz.ticketservice.dto.req.RefundTicketReqDTO;
import org.opengoofy.index12306.biz.ticketservice.dto.req.TicketPageQueryReqDTO;
import org.opengoofy.index12306.biz.ticketservice.dto.resp.RefundTicketRespDTO;
import org.opengoofy.index12306.biz.ticketservice.dto.resp.TicketOrderDetailRespDTO;
import org.opengoofy.index12306.biz.ticketservice.dto.resp.TicketPageQueryRespDTO;
import org.opengoofy.index12306.biz.ticketservice.dto.resp.TicketPurchaseRespDTO;
import org.opengoofy.index12306.biz.ticketservice.service.SeatService;
import org.opengoofy.index12306.biz.ticketservice.service.TicketService;
import org.opengoofy.index12306.biz.ticketservice.service.TrainStationService;
import org.opengoofy.index12306.biz.ticketservice.service.cache.SeatMarginCacheLoader;
import org.opengoofy.index12306.biz.ticketservice.service.handler.ticket.dto.TokenResultDTO;
import org.opengoofy.index12306.biz.ticketservice.service.handler.ticket.dto.TrainPurchaseTicketRespDTO;
import org.opengoofy.index12306.biz.ticketservice.service.handler.ticket.select.TrainSeatTypeSelector;
import org.opengoofy.index12306.biz.ticketservice.service.handler.ticket.tokenbucket.TicketAvailabilityTokenBucket;
import org.opengoofy.index12306.biz.ticketservice.toolkit.DateUtil;
import org.opengoofy.index12306.biz.ticketservice.toolkit.TimeStringComparator;
import org.opengoofy.index12306.framework.starter.bases.ApplicationContextHolder;
import org.opengoofy.index12306.framework.starter.cache.DistributedCache;
import org.opengoofy.index12306.framework.starter.cache.toolkit.CacheUtil;
import org.opengoofy.index12306.framework.starter.convention.exception.ServiceException;
import org.opengoofy.index12306.framework.starter.convention.page.PageResponse;
import org.opengoofy.index12306.framework.starter.designpattern.chain.AbstractChainContext;
import org.opengoofy.index12306.frameworks.starter.user.core.UserContext;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static org.opengoofy.index12306.biz.ticketservice.common.constant.Index12306Constant.ADVANCE_TICKET_DAY;
import static org.opengoofy.index12306.biz.ticketservice.common.constant.RedisKeyConstant.LOCK_REGION_TRAIN_STATION;
import static org.opengoofy.index12306.biz.ticketservice.common.constant.RedisKeyConstant.LOCK_REGION_TRAIN_STATION_MAPPING;
import static org.opengoofy.index12306.biz.ticketservice.common.constant.RedisKeyConstant.LOCK_TOKEN_BUCKET_ISNULL;
import static org.opengoofy.index12306.biz.ticketservice.common.constant.RedisKeyConstant.REGION_TRAIN_STATION;
import static org.opengoofy.index12306.biz.ticketservice.common.constant.RedisKeyConstant.REGION_TRAIN_STATION_MAPPING;
import static org.opengoofy.index12306.biz.ticketservice.common.constant.RedisKeyConstant.TRAIN_INFO;
import static org.opengoofy.index12306.biz.ticketservice.common.constant.RedisKeyConstant.TRAIN_STATION_PRICE;
import static org.opengoofy.index12306.biz.ticketservice.common.constant.RedisKeyConstant.TRAIN_STATION_REMAINING_TICKET;
import static org.opengoofy.index12306.biz.ticketservice.toolkit.DateUtil.convertDateToLocalTime;
import com.google.common.util.concurrent.RateLimiter;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

@Slf4j
@Service
@RequiredArgsConstructor
public class TicketServiceImpl extends ServiceImpl<TicketMapper, TicketDO> implements TicketService, CommandLineRunner {

    private final TrainMapper trainMapper;
    private final TrainStationRelationMapper trainStationRelationMapper;
    private final TrainStationPriceMapper trainStationPriceMapper;
    private final DistributedCache distributedCache;
    private final StationMapper stationMapper;
    private final SeatService seatService;
    private final TrainStationService trainStationService;
    private final TrainSeatTypeSelector trainSeatTypeSelector;
    private final SeatMarginCacheLoader seatMarginCacheLoader;
    private final AbstractChainContext<TicketPageQueryReqDTO> ticketPageQueryAbstractChainContext;
    private final RedissonClient redissonClient;
    private final ConfigurableEnvironment environment;
    private final TicketAvailabilityTokenBucket ticketAvailabilityTokenBucket;
    private TicketService ticketService;
    private final Map<String, RateLimiter> localRateLimiterMap = new ConcurrentHashMap<>();
    private final OrderMapper orderMapper;

    @Value("${ticket.availability.cache-update.type:}")
    private String ticketAvailabilityCacheUpdateType;
    @Value("${framework.cache.redis.prefix:}")
    private String cacheRedisPrefix;

    @Override
    public TicketPageQueryRespDTO pageListTicketQueryV1(TicketPageQueryReqDTO requestParam) {
        // 责任链模式 验证城市名称是否存在、不存在加载缓存以及出发日期不能小于当前日期等等
        ticketPageQueryAbstractChainContext.handler(TicketChainMarkEnum.TRAIN_QUERY_FILTER.name(), requestParam);
        StringRedisTemplate stringRedisTemplate = (StringRedisTemplate) distributedCache.getInstance();
        // 列车查询逻辑较为复杂，详细解析文章查看 https://nageoffer.com/12306/question
        // v1 版本存在严重的性能深渊问题，v2 版本完美的解决了该问题。通过 Jmeter 压测聚合报告得知，性能提升在 300% - 500%+
        List<Object> stationDetails = stringRedisTemplate.opsForHash()
                .multiGet(REGION_TRAIN_STATION_MAPPING, Lists.newArrayList(requestParam.getFromStation(), requestParam.getToStation()));
        long count = stationDetails.stream().filter(Objects::isNull).count();
        if (count > 0) {
            RLock lock = redissonClient.getLock(LOCK_REGION_TRAIN_STATION_MAPPING);
            lock.lock();
            try {
                stationDetails = stringRedisTemplate.opsForHash()
                        .multiGet(REGION_TRAIN_STATION_MAPPING, Lists.newArrayList(requestParam.getFromStation(), requestParam.getToStation()));
                count = stationDetails.stream().filter(Objects::isNull).count();
                if (count > 0) {
                    List<StationDO> stationDOList = stationMapper.selectList(Wrappers.emptyWrapper());
                    Map<String, String> regionTrainStationMap = new HashMap<>();
                    stationDOList.forEach(each -> regionTrainStationMap.put(each.getCode(), each.getRegionName()));
                    stringRedisTemplate.opsForHash().putAll(REGION_TRAIN_STATION_MAPPING, regionTrainStationMap);
                    stationDetails = new ArrayList<>();
                    stationDetails.add(regionTrainStationMap.get(requestParam.getFromStation()));
                    stationDetails.add(regionTrainStationMap.get(requestParam.getToStation()));
                }
            } finally {
                lock.unlock();
            }
        }
        List<TicketListDTO> seatResults = new ArrayList<>();
        String buildRegionTrainStationHashKey = String.format(REGION_TRAIN_STATION, stationDetails.get(0), stationDetails.get(1));
        Map<Object, Object> regionTrainStationAllMap = stringRedisTemplate.opsForHash().entries(buildRegionTrainStationHashKey);
        if (MapUtil.isEmpty(regionTrainStationAllMap)) {
            RLock lock = redissonClient.getLock(LOCK_REGION_TRAIN_STATION);
            lock.lock();
            try {
                regionTrainStationAllMap = stringRedisTemplate.opsForHash().entries(buildRegionTrainStationHashKey);
                if (MapUtil.isEmpty(regionTrainStationAllMap)) {
                    LambdaQueryWrapper<TrainStationRelationDO> queryWrapper = Wrappers.lambdaQuery(TrainStationRelationDO.class)
                            .eq(TrainStationRelationDO::getStartRegion, stationDetails.get(0))
                            .eq(TrainStationRelationDO::getEndRegion, stationDetails.get(1));
                    List<TrainStationRelationDO> trainStationRelationList = trainStationRelationMapper.selectList(queryWrapper);
                    for (TrainStationRelationDO each : trainStationRelationList) {
                        TrainDO trainDO = distributedCache.safeGet(
                                TRAIN_INFO + each.getTrainId(),
                                TrainDO.class,
                                () -> trainMapper.selectById(each.getTrainId()),
                                ADVANCE_TICKET_DAY,
                                TimeUnit.DAYS);
                        TicketListDTO result = new TicketListDTO();
                        result.setTrainId(String.valueOf(trainDO.getId()));
                        result.setTrainNumber(trainDO.getTrainNumber());
                        result.setDepartureTime(convertDateToLocalTime(each.getDepartureTime(), "HH:mm"));
                        result.setArrivalTime(convertDateToLocalTime(each.getArrivalTime(), "HH:mm"));
                        result.setDuration(DateUtil.calculateHourDifference(each.getDepartureTime(), each.getArrivalTime()));
                        result.setDeparture(each.getDeparture());
                        result.setArrival(each.getArrival());
                        result.setDepartureFlag(each.getDepartureFlag());
                        result.setArrivalFlag(each.getArrivalFlag());
                        result.setTrainType(trainDO.getTrainType());
                        result.setTrainBrand(trainDO.getTrainBrand());
                        if (StrUtil.isNotBlank(trainDO.getTrainTag())) {
                            result.setTrainTags(StrUtil.split(trainDO.getTrainTag(), ","));
                        }
                        long betweenDay = cn.hutool.core.date.DateUtil.betweenDay(each.getDepartureTime(), each.getArrivalTime(), false);
                        result.setDaysArrived((int) betweenDay);
                        result.setSaleStatus(new Date().after(trainDO.getSaleTime()) ? 0 : 1);
                        result.setSaleTime(convertDateToLocalTime(trainDO.getSaleTime(), "MM-dd HH:mm"));
                        seatResults.add(result);
                        regionTrainStationAllMap.put(CacheUtil.buildKey(String.valueOf(each.getTrainId()), each.getDeparture(), each.getArrival()), JSON.toJSONString(result));
                    }
                    stringRedisTemplate.opsForHash().putAll(buildRegionTrainStationHashKey, regionTrainStationAllMap);
                }
            } finally {
                lock.unlock();
            }
        }
        seatResults = CollUtil.isEmpty(seatResults)
                ? regionTrainStationAllMap.values().stream().map(each -> JSON.parseObject(each.toString(), TicketListDTO.class)).toList()
                : seatResults;
        seatResults = seatResults.stream().sorted(new TimeStringComparator()).toList();
        for (TicketListDTO each : seatResults) {
            String trainStationPriceStr = distributedCache.safeGet(
                    String.format(TRAIN_STATION_PRICE, each.getTrainId(), each.getDeparture(), each.getArrival()),
                    String.class,
                    () -> {
                        LambdaQueryWrapper<TrainStationPriceDO> trainStationPriceQueryWrapper = Wrappers.lambdaQuery(TrainStationPriceDO.class)
                                .eq(TrainStationPriceDO::getDeparture, each.getDeparture())
                                .eq(TrainStationPriceDO::getArrival, each.getArrival())
                                .eq(TrainStationPriceDO::getTrainId, each.getTrainId());
                        return JSON.toJSONString(trainStationPriceMapper.selectList(trainStationPriceQueryWrapper));
                    },
                    ADVANCE_TICKET_DAY,
                    TimeUnit.DAYS
            );
            List<TrainStationPriceDO> trainStationPriceDOList = JSON.parseArray(trainStationPriceStr, TrainStationPriceDO.class);
            List<SeatClassDTO> seatClassList = new ArrayList<>();
            trainStationPriceDOList.forEach(item -> {
                String seatType = String.valueOf(item.getSeatType());
                String keySuffix = StrUtil.join("_", each.getTrainId(), item.getDeparture(), item.getArrival());
                Object quantityObj = stringRedisTemplate.opsForHash().get(TRAIN_STATION_REMAINING_TICKET + keySuffix, seatType);
                int quantity = Optional.ofNullable(quantityObj)
                        .map(Object::toString)
                        .map(Integer::parseInt)
                        .orElseGet(() -> {
                            Map<String, String> seatMarginMap = seatMarginCacheLoader.load(String.valueOf(each.getTrainId()), seatType, item.getDeparture(), item.getArrival());
                            return Optional.ofNullable(seatMarginMap.get(String.valueOf(item.getSeatType()))).map(Integer::parseInt).orElse(0);
                        });
                seatClassList.add(new SeatClassDTO(item.getSeatType(), quantity, new BigDecimal(item.getPrice()).divide(new BigDecimal("100"), 1, RoundingMode.HALF_UP), false));
            });
            each.setSeatClassList(seatClassList);
        }
        return TicketPageQueryRespDTO.builder()
                .trainList(seatResults)
                .departureStationList(buildDepartureStationList(seatResults))
                .arrivalStationList(buildArrivalStationList(seatResults))
                .trainBrandList(buildTrainBrandList(seatResults))
                .seatClassTypeList(buildSeatClassList(seatResults))
                .build();
    }


    @Override
    public TicketPageQueryRespDTO pageListTicketQueryV2(TicketPageQueryReqDTO requestParam) {
        // 责任链模式 验证城市名称是否存在、不存在加载缓存以及出发日期不能小于当前日期等等
        ticketPageQueryAbstractChainContext.handler(TicketChainMarkEnum.TRAIN_QUERY_FILTER.name(), requestParam);
        StringRedisTemplate stringRedisTemplate = (StringRedisTemplate) distributedCache.getInstance();
        // 列车查询逻辑较为复杂，详细解析文章查看 https://nageoffer.com/12306/question
        // v2 版本更符合企业级高并发真实场景解决方案，完美解决了 v1 版本性能深渊问题。通过 Jmeter 压测聚合报告得知，性能提升在 300% - 500%+
        // 其实还能有 v3 版本，性能估计在原基础上还能进一步提升一倍。不过 v3 版本太过于复杂，不易读且不易扩展，就不写具体的代码了。面试中 v2 版本已经够和面试官吹的了
        List<Object> stationDetails = stringRedisTemplate.opsForHash()
                .multiGet(REGION_TRAIN_STATION_MAPPING, Lists.newArrayList(requestParam.getFromStation(), requestParam.getToStation()));
        String buildRegionTrainStationHashKey = String.format(REGION_TRAIN_STATION, stationDetails.get(0), stationDetails.get(1));
        Map<Object, Object> regionTrainStationAllMap = stringRedisTemplate.opsForHash().entries(buildRegionTrainStationHashKey);
        List<TicketListDTO> seatResults = regionTrainStationAllMap.values().stream()
                .map(each -> JSON.parseObject(each.toString(), TicketListDTO.class))
                .sorted(new TimeStringComparator())
                .toList();
        List<String> trainStationPriceKeys = seatResults.stream()
                .map(each -> String.format(cacheRedisPrefix + TRAIN_STATION_PRICE, each.getTrainId(), each.getDeparture(), each.getArrival()))
                .toList();
        List<Object> trainStationPriceObjs = stringRedisTemplate.executePipelined((RedisCallback<String>) connection -> {
            trainStationPriceKeys.forEach(each -> connection.stringCommands().get(each.getBytes()));
            return null;
        });
        List<TrainStationPriceDO> trainStationPriceDOList = new ArrayList<>();
        List<String> trainStationRemainingKeyList = new ArrayList<>();
        for (Object each : trainStationPriceObjs) {
            List<TrainStationPriceDO> trainStationPriceList = JSON.parseArray(each.toString(), TrainStationPriceDO.class);
            trainStationPriceDOList.addAll(trainStationPriceList);
            for (TrainStationPriceDO item : trainStationPriceList) {
                String trainStationRemainingKey = cacheRedisPrefix + TRAIN_STATION_REMAINING_TICKET + StrUtil.join("_", item.getTrainId(), item.getDeparture(), item.getArrival());
                trainStationRemainingKeyList.add(trainStationRemainingKey);
            }
        }
        List<Object> trainStationRemainingObjs = stringRedisTemplate.executePipelined((RedisCallback<String>) connection -> {
            for (int i = 0; i < trainStationRemainingKeyList.size(); i++) {
                connection.hashCommands().hGet(trainStationRemainingKeyList.get(i).getBytes(), trainStationPriceDOList.get(i).getSeatType().toString().getBytes());
            }
            return null;
        });
        for (TicketListDTO each : seatResults) {
            List<Integer> seatTypesByCode = VehicleTypeEnum.findSeatTypesByCode(each.getTrainType());
            List<Object> remainingTicket = new ArrayList<>(trainStationRemainingObjs.subList(0, seatTypesByCode.size()));
            List<TrainStationPriceDO> trainStationPriceDOSub = new ArrayList<>(trainStationPriceDOList.subList(0, seatTypesByCode.size()));
            trainStationRemainingObjs.subList(0, seatTypesByCode.size()).clear();
            trainStationPriceDOList.subList(0, seatTypesByCode.size()).clear();
            List<SeatClassDTO> seatClassList = new ArrayList<>();
            for (int i = 0; i < trainStationPriceDOSub.size(); i++) {
                TrainStationPriceDO trainStationPriceDO = trainStationPriceDOSub.get(i);
                SeatClassDTO seatClassDTO = SeatClassDTO.builder()
                        .type(trainStationPriceDO.getSeatType())
                        .quantity(Integer.parseInt(remainingTicket.get(i).toString()))
                        .price(new BigDecimal(trainStationPriceDO.getPrice()).divide(new BigDecimal("100"), 1, RoundingMode.HALF_UP))
                        .candidate(false)
                        .build();
                seatClassList.add(seatClassDTO);
            }
            each.setSeatClassList(seatClassList);
        }
        return TicketPageQueryRespDTO.builder()
                .trainList(seatResults)
                .departureStationList(buildDepartureStationList(seatResults))
                .arrivalStationList(buildArrivalStationList(seatResults))
                .trainBrandList(buildTrainBrandList(seatResults))
                .seatClassTypeList(buildSeatClassList(seatResults))
                .build();
    }

    /**
     * 外层：无事务，负责限流、加锁、重试
     */
    @Override
    public TicketPurchaseRespDTO purchaseTicketsV2(PurchaseTicketReqDTO requestParam) {
        // 1. 限流（保持你原来的逻辑）
        RateLimiter rateLimiter = localRateLimiterMap.computeIfAbsent(
                requestParam.getTrainId(),
                k -> RateLimiter.create(10000)
        );
        if (!rateLimiter.tryAcquire()) {
            throw new ServiceException("当前抢票人数过多，请稍后再试");
        }

        // 2. 加锁（保持你原来的本地锁 + 分布式锁逻辑）
        List<ReentrantLock> localLockList = new ArrayList<>();
        List<RLock> distributedLockList = new ArrayList<>();
        Map<Integer, List<PurchaseTicketPassengerDetailDTO>> seatTypeMap = requestParam.getPassengers().stream()
                .collect(Collectors.groupingBy(PurchaseTicketPassengerDetailDTO::getSeatType));
        seatTypeMap.forEach((seatType, count) -> {
            String lockKey = "purchase_" + requestParam.getTrainId() + "_" + seatType;
            ReentrantLock localLock = localLockMap.getIfPresent(lockKey);
            if (localLock == null) {
                synchronized (TicketService.class) {
                    if ((localLock = localLockMap.getIfPresent(lockKey)) == null) {
                        localLock = new ReentrantLock(true);
                        localLockMap.put(lockKey, localLock);
                    }
                }
            }
            localLockList.add(localLock);
            distributedLockList.add(redissonClient.getFairLock(lockKey));
        });

        try {
            localLockList.forEach(ReentrantLock::lock);
            distributedLockList.forEach(RLock::lock);

            // 3. 重试循环：每次是一个新事务
            int maxRetry = 3;
            for (int i = 0; i < maxRetry; i++) {
                try {
                    // 用代理调用，保证 executePurchaseTickets 的 @Transactional 生效
                    TicketService self = ApplicationContextHolder.getBean(TicketService.class);
                    return self.executePurchaseTickets(requestParam);
                } catch (SeatOccupiedRetryException e) {
                    log.warn("座位冲突，第 {} 次重试，原因: {}", i + 1, e.getMessage());
                }
            }
            throw new ServiceException("当前余票不足，请稍后再试");
        } finally {
            // 4. 解锁：先解分布式锁，再解本地锁
            distributedLockList.forEach(lock -> {
                try { lock.unlock(); } catch (Throwable ignored) {}
            });
            localLockList.forEach(lock -> {
                try { lock.unlock(); } catch (Throwable ignored) {}
            });
        }
    }


    private final Cache<String, ReentrantLock> localLockMap = Caffeine.newBuilder()
            .expireAfterWrite(1, TimeUnit.DAYS)
            .build();

    private final Cache<String, Object> tokenTicketsRefreshMap = Caffeine.newBuilder()
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .build();
    @Override
    @Transactional(rollbackFor = Throwable.class)
    public TicketPurchaseRespDTO executePurchaseTickets(PurchaseTicketReqDTO requestParam) {
        List<TicketOrderDetailRespDTO> ticketOrderDetailResults = new ArrayList<>();
        String trainId = requestParam.getTrainId();

        // 1. 查库
        TrainDO trainDO = trainMapper.selectById(trainId);
        if (trainDO == null) {
            throw new ServiceException("列车不存在");
        }

        // 2. 选座
        List<TrainPurchaseTicketRespDTO> trainPurchaseTicketResults = trainSeatTypeSelector.select(trainDO.getTrainType(), requestParam);

        // 3. 先生成订单号（必须放在构建 TicketDO 之前）
        String mockOrderId = "MOCK_ORDER_" + System.currentTimeMillis();

        // 4. 构建 TicketDO 列表，带上订单号
        List<TicketDO> ticketDOList = trainPurchaseTicketResults.stream()
                .map(each -> TicketDO.builder()
                        .username("test_user")
                        .trainId(Long.parseLong(requestParam.getTrainId()))
                        .carriageNumber(each.getCarriageNumber())
                        .seatNumber(each.getSeatNumber())
                        .passengerId(each.getPassengerId())
                        .ticketStatus(TicketStatusEnum.UNPAID.getCode())
                        .orderSn(mockOrderId)       // ← 新增：把订单号写进购票记录
                        .build())
                .toList();

        // 5. 本地数据库插入
        saveBatch(ticketDOList);
        // 构建订单
        OrderDO orderDO = OrderDO.builder()
                .orderSn(mockOrderId)
                .userId(UserContext.getUserId())
                .username("test_user")
                .trainId(Long.parseLong(requestParam.getTrainId()))
                .trainNumber(trainDO.getTrainNumber())
                .departure(requestParam.getDeparture())
                .arrival(requestParam.getArrival())
                .status(OrderStatusEnum.PENDING_PAYMENT.getStatus())  // 待支付
                .orderTime(new Date())// 乘车日期
                .build();

// 插入订单表
        orderMapper.insert(orderDO);

        // 6. 构建订单明细
        trainPurchaseTicketResults.forEach(each -> {
            TicketOrderDetailRespDTO ticketOrderDetailRespDTO = TicketOrderDetailRespDTO.builder()
                    .amount(each.getAmount())
                    .carriageNumber(each.getCarriageNumber())
                    .seatNumber(each.getSeatNumber())
                    .idCard(each.getIdCard())
                    .idType(each.getIdType())
                    .seatType(each.getSeatType())
                    .ticketType(each.getUserType())
                    .realName(each.getRealName())
                    .build();
            ticketOrderDetailResults.add(ticketOrderDetailRespDTO);
        });

        // 7. 返回订单号
        return new TicketPurchaseRespDTO(mockOrderId, ticketOrderDetailResults);
    }

    @Override
    public void cancelTicketOrder(CancelTicketOrderReqDTO requestParam) {
        // 1. 用 baseMapper 查购票记录
        List<TicketDO> ticketDOList = baseMapper.selectList(
                Wrappers.lambdaQuery(TicketDO.class)
                        .eq(TicketDO::getOrderSn, requestParam.getOrderSn())
        );
        if (CollUtil.isEmpty(ticketDOList)) {
            throw new ServiceException("订单不存在");
        }

        // 2. 回滚座位状态
        for (TicketDO ticketDO : ticketDOList) {
            seatService.update(
                    Wrappers.lambdaUpdate(SeatDO.class)
                            .eq(SeatDO::getTrainId, ticketDO.getTrainId())
                            .eq(SeatDO::getCarriageNumber, ticketDO.getCarriageNumber())
                            .eq(SeatDO::getSeatNumber, ticketDO.getSeatNumber())
                            .set(SeatDO::getSeatStatus, 0)
            );
        }

        // 3. 用 baseMapper 删购票记录
        baseMapper.delete(
                Wrappers.lambdaQuery(TicketDO.class)
                        .eq(TicketDO::getOrderSn, requestParam.getOrderSn())
        );
    }

    @Override
    public void payOrder(String orderSn) {
        // 1. 查订单
        OrderDO orderDO = orderMapper.selectOne(
                Wrappers.lambdaQuery(OrderDO.class)
                        .eq(OrderDO::getOrderSn, orderSn)
        );
        if (orderDO == null) {
            throw new ServiceException("订单不存在");
        }
        if (Objects.equals(orderDO.getStatus(), OrderStatusEnum.ALREADY_PAID.getStatus())) {
            throw new ServiceException("订单已支付");
        }
        if (!Objects.equals(orderDO.getStatus(), OrderStatusEnum.PENDING_PAYMENT.getStatus())) {
            throw new ServiceException("订单状态异常，无法支付");
        }

        // 2. 改订单状态
        orderMapper.update(null, Wrappers.lambdaUpdate(OrderDO.class)
                .eq(OrderDO::getOrderSn, orderSn)
                .set(OrderDO::getStatus, OrderStatusEnum.ALREADY_PAID.getStatus()));

        // 3. （可选）改 ticket 状态为已支付
        // 如果 TicketStatusEnum 里有 PAID，就在这里更新
        baseMapper.update(null, Wrappers.lambdaUpdate(TicketDO.class)
                .eq(TicketDO::getOrderSn, orderSn)
                .set(TicketDO::getTicketStatus, TicketStatusEnum.PAID.getCode()));
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public RefundTicketRespDTO commonTicketRefund(RefundTicketReqDTO requestParam) {
        String orderSn = requestParam.getOrderSn();

        // 1. 查订单
        OrderDO orderDO = orderMapper.selectOne(
                Wrappers.lambdaQuery(OrderDO.class)
                        .eq(OrderDO::getOrderSn, orderSn)
        );
        if (orderDO == null) {
            throw new ServiceException("订单不存在");
        }
        if (!Objects.equals(orderDO.getStatus(), OrderStatusEnum.ALREADY_PAID.getStatus())) {
            throw new ServiceException("订单未支付，无法退款");
        }

        // 2. 查该订单的票
        List<TicketDO> ticketDOList = baseMapper.selectList(
                Wrappers.lambdaQuery(TicketDO.class)
                        .eq(TicketDO::getOrderSn, orderSn)
        );
        if (CollUtil.isEmpty(ticketDOList)) {
            throw new ServiceException("订单无购票记录");
        }

        // 3. 恢复座位（用订单区间，只恢复卖出的那条）
        for (TicketDO ticketDO : ticketDOList) {
            seatService.update(
                    Wrappers.lambdaUpdate(SeatDO.class)
                            .eq(SeatDO::getTrainId, ticketDO.getTrainId())
                            .eq(SeatDO::getCarriageNumber, ticketDO.getCarriageNumber())
                            .eq(SeatDO::getSeatNumber, ticketDO.getSeatNumber())
                            .eq(SeatDO::getStartStation, orderDO.getDeparture())
                            .eq(SeatDO::getEndStation, orderDO.getArrival())
                            .set(SeatDO::getSeatStatus, 0)
            );
        }

        // 4. 改订单状态为全部退款
        orderMapper.update(null, Wrappers.lambdaUpdate(OrderDO.class)
                .eq(OrderDO::getOrderSn, orderSn)
                .set(OrderDO::getStatus, OrderStatusEnum.FULL_REFUND.getStatus()));

        // 5. 改票状态为退票
        baseMapper.update(null, Wrappers.lambdaUpdate(TicketDO.class)
                .eq(TicketDO::getOrderSn, orderSn)
                .set(TicketDO::getTicketStatus, TicketStatusEnum.REFUNDED.getCode()));

        // 6. 返回
        RefundTicketRespDTO resp = new RefundTicketRespDTO();
        resp.setOrderSn(orderSn);
        resp.setRefundTime(new Date());
        return resp;
    }

    @Override
    public PageResponse<OrderDO> pageOrder(Integer pageNum, Integer pageSize) {
        // 1. 构造分页对象
        Page<OrderDO> page = new Page<>(pageNum, pageSize);

        // 2. 查询
        LambdaQueryWrapper<OrderDO> wrapper = Wrappers.lambdaQuery(OrderDO.class)
                .orderByDesc(OrderDO::getOrderTime);
        Page<OrderDO> result = orderMapper.selectPage(page, wrapper);

        // 3. 转换返回
        PageResponse<OrderDO> response = new PageResponse<>();
        response.setRecords(result.getRecords());
        response.setTotal(result.getTotal());
        response.setSize(result.getSize());
        response.setCurrent(result.getCurrent());
        return response;
    }

    @Override
    public List<StationDO> listStation() {
        return stationMapper.selectList(Wrappers.emptyWrapper());
    }

    @Override
    public OrderDO queryOrderByOrderSn(String orderSn) {
        OrderDO orderDO = orderMapper.selectOne(
                Wrappers.lambdaQuery(OrderDO.class)
                        .eq(OrderDO::getOrderSn, orderSn)
        );
        if (orderDO == null) {
            throw new ServiceException("订单不存在");
        }
        return orderDO;
    }


    private List<String> buildDepartureStationList(List<TicketListDTO> seatResults) {
        return seatResults.stream().map(TicketListDTO::getDeparture).distinct().collect(Collectors.toList());
    }

    private List<String> buildArrivalStationList(List<TicketListDTO> seatResults) {
        return seatResults.stream().map(TicketListDTO::getArrival).distinct().collect(Collectors.toList());
    }

    private List<Integer> buildSeatClassList(List<TicketListDTO> seatResults) {
        Set<Integer> resultSeatClassList = new HashSet<>();
        for (TicketListDTO each : seatResults) {
            for (SeatClassDTO item : each.getSeatClassList()) {
                resultSeatClassList.add(item.getType());
            }
        }
        return resultSeatClassList.stream().toList();
    }

    private List<Integer> buildTrainBrandList(List<TicketListDTO> seatResults) {
        Set<Integer> trainBrandSet = new HashSet<>();
        for (TicketListDTO each : seatResults) {
            if (StrUtil.isNotBlank(each.getTrainBrand())) {
                trainBrandSet.addAll(StrUtil.split(each.getTrainBrand(), ",").stream().map(Integer::parseInt).toList());
            }
        }
        return trainBrandSet.stream().toList();
    }

    private final ScheduledExecutorService tokenIsNullRefreshExecutor = Executors.newScheduledThreadPool(1);

    private void tokenIsNullRefreshToken(PurchaseTicketReqDTO requestParam, TokenResultDTO tokenResult) {
        RLock lock = redissonClient.getLock(String.format(LOCK_TOKEN_BUCKET_ISNULL, requestParam.getTrainId()));
        if (!lock.tryLock()) {
            return;
        }
        tokenIsNullRefreshExecutor.schedule(() -> {
            try {
                List<Integer> seatTypes = new ArrayList<>();
                Map<Integer, Integer> tokenCountMap = new HashMap<>();
                tokenResult.getTokenIsNullSeatTypeCounts().stream()
                        .map(each -> each.split("_"))
                        .forEach(split -> {
                            int seatType = Integer.parseInt(split[0]);
                            seatTypes.add(seatType);
                            tokenCountMap.put(seatType, Integer.parseInt(split[1]));
                        });
                List<SeatTypeCountDTO> seatTypeCountDTOList = seatService.listSeatTypeCount(Long.parseLong(requestParam.getTrainId()), requestParam.getDeparture(), requestParam.getArrival(), seatTypes);
                for (SeatTypeCountDTO each : seatTypeCountDTOList) {
                    Integer tokenCount = tokenCountMap.get(each.getSeatType());
                    if (tokenCount <= each.getSeatCount()) {
                        ticketAvailabilityTokenBucket.delTokenInBucket(requestParam);
                        break;
                    }
                }
            } finally {
                lock.unlock();
            }
        }, 10, TimeUnit.SECONDS);
    }

    @Override
    public void run(String... args) throws Exception {
        ticketService = ApplicationContextHolder.getBean(TicketService.class);
    }
}
