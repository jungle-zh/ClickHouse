//
// Created by admin on 19/1/18.
//


#include <Interpreters/Expr/IdentifierExpr.h>
#include <Interpreters/Expr/FunctionExpr.h>
#include <Interpreters/Expr/LiteralExpr.h>
#include <Interpreters/Expr/ExprActions.h>
#include <Interpreters/Expr/Expr.h>
#include <Common/typeid_cast.h>
#include <Core/Names.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/convertFieldToType.h>
#include <DataTypes/FieldToDataType.h>


namespace DB {


void  Expr::getExprActions(ExprActions & actions){


  if( typeid_cast<const IdentifierExpr *>(this) )  {
      if(!actions.getSampleBlock().has(this->getName())){
          throw  Exception("not find expr :" + this->getName());
      }

  }

  if( typeid_cast<const FunctionExpr *>(this) ){

      Names argumentNames;
      DataTypes argumentTypes;
      std::string functionName = getName();

      for(size_t i=0;i< childs.size();++i){

          std::string colName = (childs)[i]->getName();
          argumentNames.push_back(colName);
          (childs)[i]->getExprActions(actions);
          argumentTypes.push_back(actions.getSampleBlock().getByName(colName).type);
      }


      ExprAction action ;
      action.applyFunction(functionName,argumentNames,argumentTypes); // add action for  current  actions
      actions.getActions()->emplace_back(std::move(action));


  } else if( const LiteralExpr * expr =  typeid_cast<const LiteralExpr *>(this) ){

      DataTypePtr type = applyVisitor(FieldToDataType(), expr->value);
      ColumnWithTypeAndName column;

      column.column = type->createColumnConst(1, convertFieldToType(expr->value, *type));
      column.type = type;
      column.name = expr->getName();

      ExprAction action;
      action.addColumn(column);
      actions.getActions()->emplace_back(std::move(action));
  } else{
      throw Exception("not expected expr type ");
  }



}


}

