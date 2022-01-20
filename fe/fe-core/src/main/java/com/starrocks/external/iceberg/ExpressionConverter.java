// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.external.iceberg;

import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BoolLiteral;
import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.DecimalLiteral;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FloatLiteral;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.IsNullPredicate;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import org.apache.iceberg.expressions.Expression;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.in;
import static org.apache.iceberg.expressions.Expressions.isNull;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.not;
import static org.apache.iceberg.expressions.Expressions.notEqual;
import static org.apache.iceberg.expressions.Expressions.notIn;
import static org.apache.iceberg.expressions.Expressions.notNull;
import static org.apache.iceberg.expressions.Expressions.or;
import static org.apache.iceberg.expressions.Expressions.startsWith;

public class ExpressionConverter {

    public static Expression toIcebergExpression(Expr expr) {
        if (expr == null) {
            return null;
        }

        if (expr instanceof CompoundPredicate) {
            CompoundPredicate compoundPredicate = (CompoundPredicate) expr;
            CompoundPredicate.Operator op = compoundPredicate.getOp();
            if (op == CompoundPredicate.Operator.NOT) {
                Expression expression = toIcebergExpression(compoundPredicate.getChild(0));
                if (expression != null) {
                    return not(expression);
                }
            } else {
                Expression left = toIcebergExpression(compoundPredicate.getChild(0));
                Expression right = toIcebergExpression(compoundPredicate.getChild(1));
                if (left != null && right != null) {
                    return (op == CompoundPredicate.Operator.OR) ? or(left, right) : and(left, right);
                }
            }
            return null;
        }

        Expr columnExpr = expr.getChild(0);
        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr functionCallExpr = (FunctionCallExpr) expr;
            if (!functionCallExpr.getFnName().getFunction().equalsIgnoreCase("starts_with")) {
                return null;
            }
            List<Expr> startsWithExprs = functionCallExpr.getParams().exprs();
            columnExpr = startsWithExprs.get(0);
        }
        if (columnExpr == null) {
            return null;
        }

        String columnName;
        if (columnExpr instanceof SlotRef) {
            columnName = ((SlotRef) columnExpr).getDesc().getColumn().getName();
        } else if (columnExpr instanceof CastExpr) {
            CastExpr columnCastExpr = (CastExpr) columnExpr;
            if (!(columnCastExpr.getChild(0) instanceof SlotRef)) {
                return null;
            }
            columnName = ((SlotRef) columnCastExpr.getChild(0)).getDesc().getColumn().getName();
        } else {
            return null;
        }

        if (expr instanceof IsNullPredicate) {
            IsNullPredicate isNullPredicate = (IsNullPredicate) expr;
            if (isNullPredicate.isNotNull()) {
                return notNull(columnName);
            } else {
                return isNull(columnName);
            }
        }

        if (expr instanceof BinaryPredicate) {
            BinaryPredicate binaryPredicate = (BinaryPredicate) expr;
            if (!(expr.getChild(1) instanceof LiteralExpr)) {
                return null;
            }
            LiteralExpr literal = (LiteralExpr) expr.getChild(1);
            Object literalValue = convertLiteral(literal);
            if (literalValue == null) {
                return null;
            }
            switch (binaryPredicate.getOp()) {
                case LT:
                    return lessThan(columnName, literalValue);
                case LE:
                    return lessThanOrEqual(columnName, literalValue);
                case GT:
                    return greaterThan(columnName, literalValue);
                case GE:
                    return greaterThanOrEqual(columnName, literalValue);
                case EQ:
                    return equal(columnName, literalValue);
                case NE:
                    return notEqual(columnName, literalValue);
                default:
                    return null;
            }
        }

        if (expr instanceof InPredicate) {
            InPredicate inPredicate = (InPredicate) expr;
            List<Expr> valuesExprList = inPredicate.getListChildren();
            List<Object> literalValues = new ArrayList<>(valuesExprList.size());
            for (Expr valueExpr : valuesExprList) {
                if (valueExpr == null || !(valueExpr instanceof LiteralExpr)) {
                    return null;
                }
                LiteralExpr literalExpr = (LiteralExpr) valueExpr;
                literalValues.add(convertLiteral(literalExpr));
            }
            if (inPredicate.isNotIn()) {
                return notIn(columnName, literalValues);
            }  else {
                return in(columnName, literalValues);
            }
        }

        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr functionCallExpr = (FunctionCallExpr) expr;
            if (!functionCallExpr.getFnName().getFunction().equalsIgnoreCase("starts_with")) {
                return null;
            }
            List<Expr> startsWithExprs = functionCallExpr.getParams().exprs();
            if (startsWithExprs.get(1) instanceof StringLiteral) {
                StringLiteral stringLiteral = (StringLiteral) startsWithExprs.get(1);
                return startsWith(columnName, stringLiteral.getStringValue());
            }
        }
        return null;
    }

    private static Object convertLiteral(LiteralExpr literalExpr) {
        if (literalExpr == null) {
            return null;
        }
        switch (literalExpr.getType().getPrimitiveType()) {
            case BOOLEAN:
                return ((BoolLiteral) literalExpr).getValue();
            case TINYINT:
            case SMALLINT:
            case INT:
                return (int) ((IntLiteral) literalExpr).getValue();
            case BIGINT:
                return ((IntLiteral) literalExpr).getValue();
            case FLOAT:
                return (float) ((FloatLiteral) literalExpr).getValue();
            case DOUBLE:
                return ((FloatLiteral) literalExpr).getValue();
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return ((DecimalLiteral) literalExpr).getValue();
            case HLL:
            case VARCHAR:
            case CHAR:
                return ((StringLiteral) literalExpr).getUnescapedValue();
            case DATE:
                return Math.toIntExact(((DateLiteral) literalExpr).toLocalDateTime().toLocalDate().toEpochDay());
            // TODO: Since the parquet reader is not compatible with int64 timestamps, this predicate pushdown may fail.
            case DATETIME:
            default:
                return null;
        }
    }
}
