package com.eagerlogic.entitydb;

import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author dipacs
 */
public final class FilterGroupItem extends AFilterItem {

    public static enum EOperator {

        AND,
        OR
    }

    private final EOperator operator;
    private final List<AFilterItem> filters;

    public FilterGroupItem(EOperator operator, AFilterItem filter1, AFilterItem filter2, AFilterItem... otherFilters) {
        if (operator == null) {
            throw new NullPointerException("The operator parameter can not be null.");
        }
        this.operator = operator;
        if (filter1 == null) {
            throw new NullPointerException("The filter1 parameter can not be null.");
        }
        if (filter2 == null) {
            throw new NullPointerException("The filter2 parameter can not be null.");
        }
        this.filters = new LinkedList<>();
        this.filters.add(filter1);
        this.filters.add(filter2);
        if (otherFilters != null) {
            for (AFilterItem filter : otherFilters) {
                if (filter == null) {
                    throw new NullPointerException("The filter in otherFilters can not be null.");
                }
                this.filters.add(filter);
            }
        }
    }

    public EOperator getOperator() {
        return operator;
    }

    List<AFilterItem> getFilters() {
        return filters;
    }

    @Override
    String getCondition() {
        String op = "OR";

        String res = null;
        boolean isFirst = true;
        for (AFilterItem filter : filters) {
            String cond = filter.getCondition();
            if (cond == null) {
                continue;
            }

            if (isFirst) {
                res = "(";
                isFirst = false;
            } else {
                res += " " + op + " ";
            }

            res += cond;
        }

        if (!isFirst) {
            res += ")";
        }

        return res;
    }

    @Override
    boolean match(Entity entity) {
        boolean res = true;
        boolean isFirst = true;
        for (AFilterItem filter : filters) {
            boolean filterRes = filter.match(entity);
            if (operator == EOperator.AND) {
                if (!filterRes) {
                    return false;
                }
            } else {
                if (isFirst) {
                    isFirst = false;
                    res = filterRes;
                } else {
                    res |= filterRes;
                }
            }
        }
        
        return res;
    }

}
