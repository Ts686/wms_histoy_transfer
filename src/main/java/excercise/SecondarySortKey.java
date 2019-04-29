package excercise;

import scala.Serializable;
import scala.math.Ordered;


public class SecondarySortKey implements Ordered<SecondarySortKey>, Serializable {
    private int first;
    private int second;

    public SecondarySortKey(int first, int second ) {
        this.first = first;
        this.second = second;
    }

    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SecondarySortKey that = (SecondarySortKey) o;

        if (first != that.first) return false;
        return second == that.second;
    }

    @Override
    public int hashCode() {
        int result = first;
        result = 31 * result + second;
        return result;
    }

    @Override
    public int compare(SecondarySortKey that) {
        if (this.first - that.first != 0) { //如果第一个字段 不相等，直接再返回 他们的比较
            return this.first - that.first;
        } else { //如果第一行相等，就比较第二行
            return this.second - that.second;
        }
    }

    @Override
    public boolean $less(SecondarySortKey that) {
        if (this.first < that.first) {
            return true;
        } else if (this.first == that.first && this.second < that.second) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater(SecondarySortKey that) {
        if (this.first > that.first) {
            return true;
        } else if (this.first == that.first && this.second > that.second) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(SecondarySortKey that) {
        if (this.$less(that)) {
            return true;
        } else if (this.first == that.first && this.second == that.second) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater$eq(SecondarySortKey that) {
        if (this.$greater(that)) {
            return true;
        } else if (this.first == that.first && this.second == that.second) {
            return true;
        }
        return false;
    }

    @Override
    public int compareTo(SecondarySortKey that) {
        if (this.first - that.first != 0) { //如果第一个字段 不相等，直接再返回 他们的比较
            return this.first - that.first;
        } else { //如果第一行相等，就比较第二行
            return this.second - that.second;
        }
    }
}
