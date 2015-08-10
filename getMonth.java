
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import java.util.regex.*;

public class getMonth  extends BaseOperation implements Function
  {
  public  getMonth( Fields fieldDeclaration )
    {
    super( 3, fieldDeclaration );
    }

  public void operate( FlowProcess flowProcess, FunctionCall functionCall )
    {
    TupleEntry argument = functionCall.getArguments();
    String loanid = argument.getString( 0 );
    String loandate = getmyMonth( argument.getString( 1 ) );
    Double unpaid_principal_balance = argument.getDouble(2);

    if(loandate.length() > 0 )
      {
      Tuple result = new Tuple();
      result.add(loanid);
      result.add( loandate );
      result.add(unpaid_principal_balance);
     // result.append( loandate );
      functionCall.getOutputCollector().add( result );
      }
    }

  public String getmyMonth(String myloanperiod )
    {

     String pattern = "(\\d+)\\/(\\d+)\\/(\\d+)";
     String monthString;

     Pattern r = Pattern.compile(pattern);
     Matcher m = r.matcher(myloanperiod);

     if  (m.find() ) {
          Integer mymonth =  Integer.parseInt(m.group(1));

  switch (mymonth) {
            case 1:  monthString = "January";
                      return monthString;

            case 2:  monthString = "February";
                      return monthString;

            case 3:  monthString = "March";
                      return monthString;

            case 4:  monthString = "April";
                      return monthString;

            case 5:  monthString = "May";
                      return monthString;

            case 6:  monthString = "June";
                      return monthString;

            case 7:  monthString = "July";
                      return monthString;

            case 8:  monthString = "August";
                      return monthString;

            case 9:  monthString = "September";
                      return monthString;

            case 10: monthString = "October";
                      return monthString;

            case 11: monthString = "November";
                      return monthString;

          case 12: monthString = "December";
                    return monthString;
    }


     }  else  {
          System.out.println("No Match found");
     }
   return  "Invalid Month" ;

  }


}
