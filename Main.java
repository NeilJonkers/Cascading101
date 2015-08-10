import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.pipe.assembly.AverageBy;
import cascading.pipe.assembly.Unique;
import cascading.operation.filter.FilterNull;
import cascading.operation.regex.*;
import cascading.operation.Filter;


public class Main  {
  public static void main( String[] args )
    {
    String dataset = args[ 0 ];   
    String outPath = args[ 1 ];



    Properties properties = new Properties();
    properties.setProperty("mapred.max.map.failures.percent","5" );


    AppProps.setApplicationJarClass( properties, Main.class );
    FlowConnector flowConnector = new Hadoop2MR1FlowConnector( properties );


 Fields fmFields = new Fields("loanid","monthly_period","servicer_name","curr_interest_rate","unpaid_principal_balance","loan_age","remaining_months_legal_maturity",
"adjusted_remaining_months_maturity","maturity_date","metropolitan_statistical_area","current_loan_delinq_status","mod_flag","zero_bal_code",
"zero_bal_effective_date","repurchase_indicator");

    // create the source tap */
    Tap inTap = new Hfs( new TextDelimited(fmFields, false, "|" ), dataset );

    // create the sink tap 
    Tap outTap = new Hfs( new TextDelimited(new Fields("monthly_period","avg-unpaid_principal_balance"),false, "\t" ), outPath );

    /** Specify a new Pipe  */
    Pipe copyPipe = new Pipe("unique copy");


    // Deal with duplicates in data set
    Pipe mypipe  = new Unique(copyPipe, new Fields("loanid","monthly_period","unpaid_principal_balance"),  Unique.Include.NO_NULLS);

 
     //define "ScrubFunction" to clean up the token stream
     Fields monthArguments = new Fields("loanid","monthly_period","unpaid_principal_balance") ;
     //mypipe = new Each( mypipe, monthArguments, new getMonth( monthArguments ), Fields.RESULTS );

    // Remove null, i.e. we discad nullsto calculate average 
    Filter filter = new RegexFilter( "(\\d+)\\.(\\d+)" );
    mypipe = new Each( mypipe,  filter );


    // Return the Month in [ January - December] , given a number [ 1 - 12 ] 
    mypipe = new Each( mypipe, monthArguments, new getMonth( monthArguments ));

    // Group by month, this need to be by month
    mypipe = new GroupBy(mypipe, new Fields("monthly_period"), new Fields("monthly_period"));

    Fields groupingFields = new Fields( "monthly_period");
    Fields valueField = new Fields( "unpaid_principal_balance" );
    Fields avgField = new Fields( "avg-unpaid_principal_balance" );

   mypipe = new AverageBy( mypipe, groupingFields, valueField, avgField );


    /**  connect the taps, pipes, etc., into a flow */
    FlowDef flowDef = FlowDef.flowDef()
     .addSource( copyPipe, inTap )
     .addTailSink(mypipe, outTap );

    /** run the flow  */
    flowConnector.connect( flowDef ).complete();
    }
  }
