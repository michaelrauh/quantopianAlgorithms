from quantopian.pipeline import Pipeline
from quantopian.algorithm import attach_pipeline, pipeline_output
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.factors import Returns


def initialize(context):
    log.info("here")
    context.x = 100

    # Create, register and name a pipeline in initialize.
    pipe = Pipeline()
    attach_pipeline(pipe, 'example')

    # Construct a simple moving average factor and add it to the pipeline.
    simple_return = Returns(inputs=[USEquityPricing.close], window_length=365)
    pipe.add(simple_return, 'simple_return')

    # Set a screen on the pipelines to filter out securities.
    pipe.set_screen(simple_return > 1.0)
    
    schedule_function(func=rebalance, date_rule = date_rules.every_day(), time_rule = time_rules.market_open(hours = 1))
def before_trading_start(context, data):
    # Pipeline_output returns the constructed dataframe.
    output = pipeline_output('example')

    # Select and update your universe.
    context.my_universe = output.sort('simple_return', ascending=False).iloc[:context.x]
    update_universe(context.my_universe.index)


def handle_data(context, data):
    log.info("\n attempting to order" + str(context.my_universe.head(context.x)))
    
def rebalance(context,data):
    weight = 1.0/context.x
    
    open_orders = get_open_orders()
    
    for stock in context.portfolio.positions.iterkeys():
        if stock not in context.my_universe.index and stock not in open_orders:
            order_target(stock, 0)
    
    for long_stock in context.my_universe.index:
        if long_stock in data:
            if long_stock not in open_orders:
                log.info("ordering " + str(long_stock))
                order_target_percent(long_stock, weight)

