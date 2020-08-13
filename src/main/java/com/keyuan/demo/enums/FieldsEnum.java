package com.keyuan.demo.enums;

/**
 * @ClassName FieldsEnum
 * @Description
 * @Author huangkeyuan
 * @Date 2020-08-12 17:01
 * @Version 1.0
 */
public enum  FieldsEnum {
    // ES的字段类型
    TEXT("text"),
    LONG("long"),
    DOUBLE("double"),
    DATE("date"),
    OTHER("");

    private String value;

    private FieldsEnum(String value){
        this.value = value;
    }
    public String value(){
        return value;
    }
}
