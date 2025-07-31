package com.sky.service.impl;

import com.sky.dto.GoodsSalesDTO;
import com.sky.entity.Orders;
import com.sky.mapper.OrderMapper;
import com.sky.mapper.UserMapper;
import com.sky.service.ReportService;
import com.sky.vo.OrderReportVO;
import com.sky.vo.SalesTop10ReportVO;
import com.sky.vo.TurnoverReportVO;
import com.sky.vo.UserReportVO;
import io.swagger.models.auth.In;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;
import java.sql.Date;

@Service
@Slf4j
public class ReportServicImpl implements ReportService {
    @Autowired
    private OrderMapper orderMapper;
    @Autowired
    private UserMapper userMapper;

    /**
     * 统计指定时间区间内的营业额数据
     *
     * @param begin
     * @param end
     * @return
     */
    /*public TurnoverReportVO getTurnoverStatistics(LocalDate begin, LocalDate end) {
        List<LocalDate> dateList = new ArrayList<>();
        dateList.add(begin);
        LocalDate current = begin;
        while (current.isBefore(end)) { // 当 current 在 end 之前时，循环继续
            current = current.plusDays(1); // 将当前日期向后推一天
            dateList.add(current);         // 将新的日期加入列表
        }

        List<Double> turnoverList = new ArrayList<>();
        for (LocalDate date : dateList) {
            LocalDateTime beginTime = LocalDateTime.of(date, LocalTime.MIN);
            LocalDateTime endTime = LocalDateTime.of(date, LocalTime.MAX);
            Map map = new HashMap();
            map.put("begin",beginTime);
            map.put("end",endTime);
            map.put("status", Orders.COMPLETED);
            Double turnover = orderMapper.sumByMap(map);
            turnover = turnover == null ? 0.0 : turnover;
            turnoverList.add(turnover);
        }

        return TurnoverReportVO.builder()
                .dateList(StringUtils.join(dateList, ","))
                .turnoverList(StringUtils.join(turnoverList,","))
                .build();
    }*/
    public TurnoverReportVO getTurnoverStatistics(LocalDate begin, LocalDate end) {
        // 依然生成日期列表，用于确保所有日期都有数据，即使某天没有营业额也显示0
        List<LocalDate> dateList = new ArrayList<>();
        LocalDate current = begin;
        while (!current.isAfter(end)) { // 包含 begin 和 end
            dateList.add(current);
            current = current.plusDays(1);
        }

        // 调用一个新方法，一次性获取所有日期的营业额
        // orderMapper.getDailyTurnovers(begin, end, Orders.COMPLETED)
        List<Map<String, Object>> dailyTurnoverResults = orderMapper.getDailyTurnovers(
                LocalDateTime.of(begin, LocalTime.MIN),
                LocalDateTime.of(end, LocalTime.MAX),
                Orders.COMPLETED
        );

        Map<LocalDate, Double> turnoverMap = dailyTurnoverResults.stream()
                .collect(Collectors.toMap(
                        map -> {
                            // 安全地将 java.sql.Date 转换为 LocalDate
                            Object orderDateObj = map.get("orderDate");
                            if (orderDateObj instanceof Date) {
                                // 方法 1: 推荐。使用 java.sql.Date 的 getTime() 获取毫秒值，然后转换为 LocalDate
                                // 这是更通用的方法，不受 toInstant() 实现的限制
                                return new java.util.Date(((Date) orderDateObj).getTime())
                                        .toInstant()
                                        .atZone(ZoneId.systemDefault())
                                        .toLocalDate();
                            } else if (orderDateObj instanceof LocalDate) {
                                // 如果 Mybatis 已经直接映射为 LocalDate，则直接返回
                                return (LocalDate) orderDateObj;
                            } else if (orderDateObj instanceof java.util.Date) {
                                // 如果 Mybatis 映射为 java.util.Date
                                return ((java.util.Date) orderDateObj)
                                        .toInstant()
                                        .atZone(ZoneId.systemDefault())
                                        .toLocalDate();
                            }
                            // 如果是其他类型，或者为 null，你可能需要根据实际情况处理
                            // 例如，如果 orderDateObj 是 null，这里会抛出 NullPointerException
                            // 确保数据库查询结果不为 null，或者在这里进行 null 检查
                            throw new IllegalArgumentException("Unsupported type for orderDate: " + (orderDateObj != null ? orderDateObj.getClass().getName() : "null"));
                        },
                        map -> {
                            // 确保 dailyTurnover 是 BigDecimal 或 Double
                            Object dailyTurnoverObj = map.get("dailyTurnover");
                            if (dailyTurnoverObj instanceof BigDecimal) {
                                return ((BigDecimal) dailyTurnoverObj).doubleValue();
                            } else if (dailyTurnoverObj instanceof Double) {
                                return (Double) dailyTurnoverObj;
                            }
                            // 处理其他可能类型或 null 值
                            return 0.0; // 默认值
                        }
                ));
        List<Double> turnoverList = new ArrayList<>();
        for (LocalDate date : dateList) {
            turnoverList.add(turnoverMap.getOrDefault(date, 0.0)); // 如果某天没有数据，则为0
        }

        return TurnoverReportVO.builder()
                .dateList(StringUtils.join(dateList, ","))
                .turnoverList(StringUtils.join(turnoverList, ","))
                .build();
    }

    /**
     * 统计指定时间区间内的用户数据
     *
     * @param begin
     * @param end
     * @return
     */
    public UserReportVO getUserStatistics(LocalDate begin, LocalDate end) {
        List<LocalDate> dateList = new ArrayList<>();
        dateList.add(begin);
        LocalDate current = begin;
        while (!current.equals(end)) {
            current = current.plusDays(1);
            dateList.add(current);
        }

        List<Integer> newUserList = new ArrayList<>();
        List<Integer> totalUserList = new ArrayList<>();

        for (LocalDate date : dateList) {
            LocalDateTime beginTime = LocalDateTime.of(date, LocalTime.MIN);
            LocalDateTime endTime = LocalDateTime.of(date, LocalTime.MAX);
            HashMap map = new HashMap();
            map.put("end", endTime);
            Integer totalUser = userMapper.countByMap(map);
            map.put("begin", beginTime);
            Integer newUser = userMapper.countByMap(map);
            totalUserList.add(totalUser);
            newUserList.add(newUser);
        }

        return UserReportVO.builder()
                .dateList(StringUtils.join(dateList, ","))
                .totalUserList(StringUtils.join(totalUserList, ","))
                .newUserList(StringUtils.join(newUserList, ","))
                .build();
    }

    /**
     * 统计指定时间区间内的订单数据
     *
     * @param begin
     * @param end
     * @return
     */
    public OrderReportVO getOrderStatistics(LocalDate begin, LocalDate end) {
        List<LocalDate> dateList = new ArrayList<>();
        dateList.add(begin);
        LocalDate current = begin;
        while (!current.equals(end)) {
            current = current.plusDays(1);
            dateList.add(current);
        }

        List<Integer> orderCountList = new ArrayList<>();
        List<Integer> validOrderCountList = new ArrayList<>();

        for (LocalDate date : dateList) {
            LocalDateTime beginTime = LocalDateTime.of(date, LocalTime.MIN);
            LocalDateTime endTime = LocalDateTime.of(date, LocalTime.MAX);
            Integer orderCount = getOrderCount(beginTime, endTime, null);
            Integer validOrderCount = getOrderCount(beginTime, endTime, Orders.COMPLETED);

            orderCountList.add(orderCount);
            validOrderCountList.add(validOrderCount);
        }

        Integer totalOrderCount = orderCountList.stream().reduce(Integer::sum).get();
        Integer validOrderCount = validOrderCountList.stream().reduce(Integer::sum).get();

        Double orderCompletionRate = totalOrderCount == 0 ? 0.0 : (validOrderCount.doubleValue() / totalOrderCount);

        return OrderReportVO.builder()
                .dateList(StringUtils.join(dateList, ","))
                .orderCountList(StringUtils.join(orderCountList, ","))
                .validOrderCountList(StringUtils.join(validOrderCountList, ","))
                .totalOrderCount(totalOrderCount)
                .validOrderCount(validOrderCount)
                .orderCompletionRate(orderCompletionRate)
                .build();
    }

    /**
     * 根据条件统计订单数量
     *
     * @param begin
     * @param end
     * @param status
     * @return
     */
    private Integer getOrderCount(LocalDateTime begin, LocalDateTime end, Integer status) {
        Map map = new HashMap();
        map.put("begin", begin);
        map.put("end", end);
        map.put("status", status);

        return orderMapper.countByMap(map);
    }

    /**
     * 统计指定时间区间内的销量排名前10
     *
     * @param begin
     * @param end
     * @return
     */
    public SalesTop10ReportVO getSalesTop10(LocalDate begin, LocalDate end) {
        LocalDateTime beginTime = LocalDateTime.of(begin, LocalTime.MIN);
        LocalDateTime endTime = LocalDateTime.of(end, LocalTime.MAX);
        List<GoodsSalesDTO> salesTop10 = orderMapper.getSalesTop10(beginTime, endTime);
        List<String> names = salesTop10.stream().map(GoodsSalesDTO::getName).collect(Collectors.toList());
        List<Integer> numbers = salesTop10.stream().map(GoodsSalesDTO::getNumber).collect(Collectors.toList());
        String nameList = StringUtils.join(names, ",");
        String numberList = StringUtils.join(numbers, ",");
        return SalesTop10ReportVO.builder()
                .nameList(nameList)
                .numberList(numberList)
                .build();
    }
}
