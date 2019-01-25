//
// Created by admin on 19/1/18.
//

#ifndef CLICKHOUSE_EXPRACTION_H
#define CLICKHOUSE_EXPRACTION_H

#include <string>
#include <Core/Names.h>
#include <DataTypes/IDataType.h>
#include <Core/ColumnWithTypeAndName.h>

namespace DB {

class ExprAction {

public:


    void  applyFunction(std::string functionName,Names argumentNames, DataTypes argumentTypes) ;


    void addColumn(ColumnWithTypeAndName & column);




};

}




#endif //CLICKHOUSE_EXPRACTION_H
