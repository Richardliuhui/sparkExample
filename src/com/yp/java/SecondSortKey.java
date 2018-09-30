package com.yp.java;

import scala.math.Ordered;

/**
 * @author liuhui
 * @date 2018-09-20 下午5:39
 */
public class SecondSortKey implements Ordered<SecondSortKey>,java.io.Serializable {
    private Integer first;
    private Integer second;

    public SecondSortKey(Integer first, Integer second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public boolean $greater$eq(SecondSortKey other) {
       if($greater(other)){
           return true;
       }else if(first==other.getFirst().intValue() && second==other.getSecond().intValue()){
           return true;
       }
        return false;
    }

    @Override
    public boolean $greater(SecondSortKey other) {
        if(this.first>other.getFirst()){
            return true;
        }else if(this.first==other.getFirst().intValue() && second>other.getSecond()){
            return true;
        }
        return false;

    }

    @Override
    public int compare(SecondSortKey key) {
        if(first-key.getFirst()!=0){
            return first-key.getFirst();
        }else{
            return second-key.getSecond();
        }
    }

    @Override
    public int compareTo(SecondSortKey key) {
        if(first-key.getFirst()!=0){
            return first-key.getFirst();
        }else{
            return second-key.getSecond();
        }
    }

    @Override
    public boolean $less$eq(SecondSortKey other) {
        if($less(other)){
            return true;
        }else if(first==other.getFirst().intValue() && second==other.getSecond().intValue()){
            return true;
        }
        return false;
    }

    @Override
    public boolean $less(SecondSortKey other) {
        if(this.first<other.getFirst()){
            return true;
        }else if(this.first==other.getFirst().intValue() && second<other.getSecond()){
            return true;
        }
        return false;
    }

    public Integer getFirst() {
        return first;
    }

    public void setFirst(Integer first) {
        this.first = first;
    }

    public Integer getSecond() {
        return second;
    }

    public void setSecond(Integer second) {
        this.second = second;
    }
}
