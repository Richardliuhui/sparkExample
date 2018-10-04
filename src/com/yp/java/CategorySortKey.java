package com.yp.java;

import scala.math.Ordered;

/**
 * @Project: scala-test
 * @Package com.yp.java
 * @Description: 品类二次排序
 * @date Date : 2018年10月04日 上午9:37
 */
public class CategorySortKey implements Ordered<CategorySortKey> {

    private long clickCount;
    private long orderCount;
    private long payCount;

    @Override
    public int compare(CategorySortKey that) {
        if(clickCount-that.clickCount!=0){
            return (int)(clickCount-that.clickCount);
        }else if(orderCount-that.orderCount!=0){
            return (int)(orderCount-that.orderCount);
        }else if(payCount-that.payCount!=0){
            return (int)(payCount-that.payCount);
        }
        return 0;
    }

    @Override
    public boolean $less(CategorySortKey that) {
        if(clickCount<that.clickCount){
            return true;
        }else if(clickCount==that.clickCount && orderCount<that.orderCount){
            return true;
        }else if(clickCount==that.clickCount &&
                orderCount==that.orderCount &&
                payCount<that.payCount){
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater(CategorySortKey that) {
        if(clickCount>that.clickCount){
            return true;
        }else if(clickCount==that.clickCount && orderCount>that.orderCount){
            return true;
        }else if(clickCount==that.clickCount &&
                orderCount==that.orderCount &&
                payCount>that.payCount){
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(CategorySortKey that) {
        if($less(that)){
            return true;
        }else if(clickCount==that.clickCount &&
                orderCount==that.orderCount &&
                payCount==that.payCount){
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater$eq(CategorySortKey that) {
        if($greater(that)){
            return true;
        }else if(clickCount==that.clickCount &&
                orderCount==that.orderCount &&
                payCount==that.payCount){
            return true;
        }
        return false;
    }

    @Override
    public int compareTo(CategorySortKey that) {
        return compare(that);
    }

    public long getClickCount() {
        return clickCount;
    }

    public void setClickCount(long clickCount) {
        this.clickCount = clickCount;
    }

    public long getOrderCount() {
        return orderCount;
    }

    public void setOrderCount(long orderCount) {
        this.orderCount = orderCount;
    }

    public long getPayCount() {
        return payCount;
    }

    public void setPayCount(long payCount) {
        this.payCount = payCount;
    }
}
