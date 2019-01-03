#pragma once

#include <Parsers/IAST.h>
#include <Core/Names.h>
#include <Parsers/ASTTablesInSelectQuery.h>


namespace DB
{



/** JOIN SELECT query
  */
class ASTJoinSelectQuery : public IAST
{
public:
    /** Get the text that identifies this element. */
    String getID() const override { return "JoinSelectQuery"; };

    ASTPtr clone() const override;

    bool containsJoin(){ return false;}

    ASTPtr getLeft(){
        return left;
    }

    ASTPtr getRight(){
        return  rigth;
    }


    ASTTableJoin getJoin(){
        return  join;
    }




protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;


private:
    ASTPtr left;
    ASTPtr rigth ;

    ASTTableJoin join;


};

}
