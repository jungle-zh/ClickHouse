//
// Created by jungle on 19-7-21.
//

#include <Columns/ColumnsCommon.h>
#include "FilterExecNode.h"

namespace DB {

    void FilterExecNode::readPrefix(std::shared_ptr<DataExechangeClient> client) {
        (void) client;
        //children.push_back(input);

        /// Determine position of filter column.
        header = children->getHeader(false);
        inputHeader = header;
        expression->execute(header);

        filter_column = header.getPositionByName(filter_column_name);
        auto & column_elem = header.safeGetByPosition(filter_column);

        /// Isn't the filter already constant?
        if (column_elem.column)
            constant_filter_description = ConstantFilterDescription(*column_elem.column);

        if (!constant_filter_description.always_false
            && !constant_filter_description.always_true)
        {
            /// Replace the filter column to a constant with value 1.
            FilterDescription filter_description_check(*column_elem.column);
            column_elem.column = column_elem.type->createColumnConst(header.rows(), UInt64(1));
        }
    }

    Block FilterExecNode::read() {

        Block res;

        if (constant_filter_description.always_false)
            return res;

        /// Until non-empty block after filtering or end of stream.
        while (1)
        {
            res = children->read();
            if (!res)
                return res;

            expression->execute(res);

            if (constant_filter_description.always_true)
                return res;

            size_t columns = res.columns();
            ColumnPtr column = res.safeGetByPosition(filter_column).column;

            /** It happens that at the stage of analysis of expressions (in sample_block) the columns-constants have not been calculated yet,
                *  and now - are calculated. That is, not all cases are covered by the code above.
                * This happens if the function returns a constant for a non-constant argument.
                * For example, `ignore` function.
                */
            constant_filter_description = ConstantFilterDescription(*column);

            if (constant_filter_description.always_false)
            {
                res.clear();
                return res;
            }

            if (constant_filter_description.always_true)
                return res;

            FilterDescription filter_and_holder(*column);

            /** Let's find out how many rows will be in result.
              * To do this, we filter out the first non-constant column
              *  or calculate number of set bytes in the filter.
              */
            size_t first_non_constant_column = 0;
            for (size_t i = 0; i < columns; ++i)
            {
                if (!res.safeGetByPosition(i).column->isColumnConst())
                {
                    first_non_constant_column = i;

                    if (first_non_constant_column != static_cast<size_t>(filter_column))
                        break;
                }
            }

            size_t filtered_rows = 0;
            if (first_non_constant_column != static_cast<size_t>(filter_column))
            {
                ColumnWithTypeAndName & current_column = res.safeGetByPosition(first_non_constant_column);
                current_column.column = current_column.column->filter(*filter_and_holder.data, -1);
                filtered_rows = current_column.column->size();
            }
            else
            {
                filtered_rows = countBytesInFilter(*filter_and_holder.data);
            }

            /// If the current block is completely filtered out, let's move on to the next one.
            if (filtered_rows == 0)
                continue;

            /// If all the rows pass through the filter.
            if (filtered_rows == filter_and_holder.data->size())
            {
                /// Replace the column with the filter by a constant.
                res.safeGetByPosition(filter_column).column = res.safeGetByPosition(filter_column).type->createColumnConst(filtered_rows, UInt64(1));
                /// No need to touch the rest of the columns.
                return res;
            }

            /// Filter the rest of the columns.
            for (size_t i = 0; i < columns; ++i)
            {
                ColumnWithTypeAndName & current_column = res.safeGetByPosition(i);

                if (i == static_cast<size_t>(filter_column))
                {
                    /// The column with filter itself is replaced with a column with a constant `1`, since after filtering, nothing else will remain.
                    /// NOTE User could pass column with something different than 0 and 1 for filter.
                    /// Example:
                    ///  SELECT materialize(100) AS x WHERE x
                    /// will work incorrectly.
                    current_column.column = current_column.type->createColumnConst(filtered_rows, UInt64(1));
                    continue;
                }

                if (i == first_non_constant_column)
                    continue;

                if (current_column.column->isColumnConst())
                    current_column.column = current_column.column->cut(0, filtered_rows);
                else
                    current_column.column = current_column.column->filter(*filter_and_holder.data, -1);
            }

            return res;
        }

    }

    void FilterExecNode::serialize(DB::WriteBuffer &buffer) {

        writeStringBinary(filter_column_name, buffer);
        ExecNode::serializeExpressActions(*expression,buffer);
        ExecNode::serializeHeader(inputHeader,buffer);

    }

    std::shared_ptr<ExecNode> FilterExecNode::deserialize(DB::ReadBuffer &buffer, Context * context) {

        std::string filter_column_name ;
        readStringBinary(filter_column_name,buffer);
        std::shared_ptr<ExpressionActions> expression = ExecNode::deSerializeExpressActions(buffer,context);
        Block inputHeader = ExecNode::deSerializeHeader(buffer);
        return  std::make_shared<FilterExecNode>(filter_column_name,expression,inputHeader);

    }


}
