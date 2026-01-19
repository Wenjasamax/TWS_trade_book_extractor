from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.execution import ExecutionFilter
import pandas as pd
from datetime import datetime
import time
import os
import threading

class TradingApp(EWrapper, EClient):
    def __init__(self, client_id, port):
        EClient.__init__(self, self)
        self.client_id = client_id
        self.port = port
        self.executions = []
        self.commission_report = {}
        self.is_ready = False
        
    def execDetails(self, reqId, contract, execution):
        super().execDetails(reqId, contract, execution)
        self.executions.append({
            'contract': contract,
            'execution': execution
        })
    
    def execDetailsEnd(self, reqId):
        super().execDetailsEnd(reqId)
        self.is_ready = True
        
    def commissionReport(self, commreport):
        super().commissionReport(commreport)
        self.commission_report[commreport.execId] = {
            'commission': commreport.commission,
            'currency': commreport.currency,
            'realizedPNL': commreport.realizedPNL
        }

def get_trade_data_from_connection(port, client_id):
    """Connect to a single TWS instance and get trade data"""
    app = TradingApp(client_id, port)
    
    try:
        print(f"Connecting to TWS on port {port} (Client ID: {client_id})...")
        app.connect('127.0.0.1', port, clientId=client_id)
        
        # Start the socket in a thread
        api_thread = threading.Thread(target=app.run, daemon=True)
        api_thread.start()
        
        # Give it a moment to connect
        time.sleep(1)
        
        if not app.isConnected():
            print(f"Failed to connect to TWS on port {port}")
            return []
        
        # Request executions
        print(f"Fetching trade data from port {port}...")
        app.reqExecutions(1, ExecutionFilter())
        
        # Wait for data to be received
        max_wait = 10
        waited = 0
        while not app.is_ready and waited < max_wait:
            time.sleep(0.5)
            waited += 0.5
        
        # Additional time to ensure all data is received
        time.sleep(1)
        
        trade_data = process_executions(app)
        
        return trade_data
        
    except Exception as e:
        print(f"Error connecting to port {port}: {str(e)}")
        return []
    finally:
        if app.isConnected():
            app.disconnect()

def process_executions(app):
    """Process execution data from app"""
    trade_data = []
    
    for exec_info in app.executions:
        contract = exec_info['contract']
        execution = exec_info['execution']
        
        # Get account number
        account = execution.acctNumber
        
        # Determine action (BOT, SLD)
        action = 'BOT' if execution.side == 'BOT' else 'SLD'
        
        # Get execution time and format as DD.MM.YYYY HH:MM:SS
        try:
            dt = datetime.strptime(execution.time, '%Y%m%d  %H:%M:%S')
            exec_time = dt.strftime('%d.%m.%Y %H:%M:%S')
        except Exception as e:
            print(f"Error formatting date: {e}")
            exec_time = execution.time
        
        # Get quantity
        quantity = execution.shares
        
        # Get symbol and security type
        symbol = contract.symbol
        sec_type = contract.secType
        
        # Initialize fields
        option_expiry = ''
        option_strike = ''
        option_right = ''
        currency = contract.currency
        price = execution.price
        
        if sec_type == 'STK':
            security_info = 'STOCK'
        elif sec_type == 'OPT' or sec_type == 'FOP':
            expiry_date_str = contract.lastTradeDateOrContractMonth
            year = int(expiry_date_str[:4])
            month = int(expiry_date_str[4:6])
            day = int(expiry_date_str[6:8])
            month_names = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 
                         'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
            formatted_date = f"{month_names[month-1]}'{day:02d}'{str(year)[-2:]}"
            
            option_strike = contract.strike
            option_right = 'CALL' if contract.right == 'C' else 'PUT'
            security_info = f"{formatted_date} {option_strike} {option_right}"
        
        # Get commission and realized P&L
        commission_value = 0.0
        realized_pnl_from_tws = None
        if execution.execId in app.commission_report:
            commission_value = app.commission_report[execution.execId]['commission']
            realized_pnl_from_tws = app.commission_report[execution.execId].get('realizedPNL')

        if price == 0 and commission_value == 0:
            action = 'EXPIRED'

        unrealized_pnl = ''
        realized_pnl = ''
        
        order_ref = getattr(execution, 'orderRef', '')
        
        if 'OptTrader' in str(order_ref):
            unrealized_pnl = (price * 100) - commission_value
        
        MAX_FLOAT_SENTINEL = 1.7976931348623157e+308
        
        if realized_pnl_from_tws is not None:
            if isinstance(realized_pnl_from_tws, float):
                if abs(realized_pnl_from_tws) != MAX_FLOAT_SENTINEL:
                    try:
                        realized_pnl = float(realized_pnl_from_tws)
                    except (ValueError, TypeError):
                        realized_pnl = ''
            else:
                try:
                    realized_pnl = float(realized_pnl_from_tws)
                except (ValueError, TypeError):
                    realized_pnl = ''
            
        trade_data.append({
            'Account': account,
            'Action': action,
            'Date_Time': exec_time,
            'Quantity': quantity,
            'Symbol': symbol,
            'Security_Info': security_info,
            'Currency': currency,
            'Price': price,
            'Commission': commission_value,
            'Unrealized_PnL': unrealized_pnl,
            'Realized_PnL': realized_pnl if realized_pnl != '' else '',
            'Exchange': contract.exchange
        })
    
    return trade_data

def mark_assigned_options(trade_data):
    """Mark options as ASSIGNED if they match stock trades with same symbol and datetime"""
    stock_trades = {}
    
    for trade in trade_data:
        if trade['Security_Info'] == 'STOCK':
            key = (trade['Symbol'], trade['Date_Time'])
            if key not in stock_trades:
                stock_trades[key] = []
            stock_trades[key].append(trade)
    
    for trade in trade_data:
        if trade['Security_Info'] != 'STOCK' and trade['Price'] == 0 and trade['Commission'] == 0:
            key = (trade['Symbol'], trade['Date_Time'])
            if key in stock_trades:
                trade['Action'] = 'ASSIGNED'
            else:
                if trade['Action'] not in ['ASSIGNED', 'BOT', 'SLD']:
                    trade['Action'] = 'EXPIRED'
    
    return trade_data

def process_combos(trade_data):
    """Process combo trades"""
    from collections import defaultdict
    
    trade_groups = defaultdict(list)
    stocks_trades = []
    parse_error_trades = []
    
    for trade in trade_data:
        if trade['Security_Info'] == 'STOCKS':
            stocks_trades.append(trade)
            continue
            
        try:
            trade_time = datetime.strptime(trade['Date_Time'], '%d.%m.%Y %H:%M:%S')
            time_key = (trade['Symbol'], trade_time.strftime('%d.%m.%Y %H:%M:%S'))
            trade_groups[time_key].append(trade)
        except Exception as e:
            parse_error_trades.append(trade)
    
    processed_trades = []
    for (symbol, time_str), trades in trade_groups.items():
        if len(trades) <= 1:
            for trade in trades:
                processed_trades.append(trade)
            continue
        
        has_negative = any(trade['Price'] < 0 for trade in trades)
        
        if has_negative:
            non_smart_trades = [t for t in trades if t.get('Exchange') != 'SMART']
            
            try:
                sorted_trades = sorted(non_smart_trades, key=lambda x: float(x['Security_Info'].split()[1])) if non_smart_trades else []
                strikes = [float(trade['Security_Info'].split()[1]) for trade in sorted_trades]
                types = [trade['Security_Info'].split()[2] for trade in sorted_trades]
                expiry_date = sorted_trades[0]['Security_Info'].split()[0] if sorted_trades else ''
                
                if strikes and expiry_date:
                    first_two_strikes = [str(int(s)) for s in sorted(strikes, reverse=True)[:2]]
                    strike_str = '/'.join(first_two_strikes)
                    combined_security_info = f"{expiry_date} {strike_str}"
                else:
                    combined_security_info = ''
                
                pnl_sum = 0.0
                for trade in non_smart_trades:
                    if trade.get('Realized_PnL') and trade['Realized_PnL'] != '':
                        try:
                            pnl_sum += float(trade['Realized_PnL'])
                        except (ValueError, TypeError):
                            pass
                
                for trade in trades:
                    if trade.get('Exchange') == 'SMART':
                        if combined_security_info:
                            trade['Security_Info'] = combined_security_info
                        if pnl_sum != 0:
                            current_pnl = trade.get('Realized_PnL', '')
                            if current_pnl and current_pnl != '':
                                try:
                                    trade['Realized_PnL'] = float(current_pnl) + pnl_sum
                                except (ValueError, TypeError):
                                    trade['Realized_PnL'] = pnl_sum
                            else:
                                trade['Realized_PnL'] = pnl_sum
                        processed_trades.append(trade)
                
                if not any(t.get('Exchange') == 'SMART' for t in trades):
                    for trade in trades:
                        processed_trades.append(trade)
                        
            except (IndexError, ValueError) as e:
                for trade in trades:
                    processed_trades.append(trade)
        else:
            for trade in trades:
                processed_trades.append(trade)
    
    processed_trades.extend(stocks_trades)
    processed_trades.extend(parse_error_trades)
    
    return processed_trades

def save_to_excel(data, filepath):
    """Save trade data to Excel file"""
    if not data:
        print("No trade data to save.")
        return
    
    data = mark_assigned_options(data)
    processed_data = process_combos(data)
    
    df = pd.DataFrame(processed_data)
    
    columns = [
        'Account', 'Action', 'Date_Time', 'Quantity', 'Symbol',
        'Security_Info', 'Currency', 'Price', 'Commission',
        'Unrealized_PnL', 'Realized_PnL', 'Exchange'
    ]
    
    columns = [col for col in columns if col in df.columns]
    df = df[columns]
    
    try:
        if os.path.exists(filepath):
            existing_df = pd.read_excel(filepath)
            df = pd.concat([existing_df, df], ignore_index=True)
            print(f"Appending {len(processed_data)} new records to existing file...")
        else:
            print(f"Creating new file with {len(processed_data)} records...")
        
        df.to_excel(filepath, index=False)
        print(f"Data successfully saved to {filepath}")
    except Exception as e:
        print(f"Error saving to Excel: {str(e)}")

def main():
    # List of TWS ports to connect to
    # Change these ports according to your TWS settings
    ports = ["YOUR PORT"]  # Example: [3714, 7297, 5468] for 3 different accounts
    
    all_trade_data = []
    
    # Connect to all ports and collect data
    for i, port in enumerate(ports):
        try:
            trade_data = get_trade_data_from_connection(port, i + 1)
            if trade_data:
                all_trade_data.extend(trade_data)
                print(f"Successfully retrieved {len(trade_data)} trades from port {port}")
            else:
                print(f"No trades found on port {port}")
        except Exception as e:
            print(f"Error processing port {port}: {str(e)}")
    
    if all_trade_data:
        output_file = r"YOUR FILE PATH\trade_data.xlsx"  # Change to your desired output path
        save_to_excel(all_trade_data, output_file)
        print(f"\nTotal trades exported: {len(all_trade_data)}")
    else:
        print("\nNo trade data collected from any account.")

if __name__ == "__main__":
    main()
