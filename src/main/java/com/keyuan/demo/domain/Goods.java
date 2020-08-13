package com.keyuan.demo.domain;

import lombok.Data;

import java.math.BigDecimal;

/**
 * @ClassName Goods
 * @Description
 * @Author huangkeyuan
 * @Date 2020-08-12 17:00
 * @Version 1.0
 */
@Data
public class Goods {
    private String id;

    private String name;

    private BigDecimal price;

    private Long skuId;

    private Long spuId;

    private Long colorId;

    private String colorName;

    public Goods() {
    }

    public Goods(String id, String name, BigDecimal price, Long skuId, Long spuId, Long colorId, String colorName) {
        this.id = id;
        this.name = name;
        this.price = price;
        this.skuId = skuId;
        this.spuId = spuId;
        this.colorId = colorId;
        this.colorName = colorName;
    }

    public Goods(String name, BigDecimal price, Long skuId, Long spuId, Long colorId, String colorName) {
        this.name = name;
        this.price = price;
        this.skuId = skuId;
        this.spuId = spuId;
        this.colorId = colorId;
        this.colorName = colorName;
    }
}
