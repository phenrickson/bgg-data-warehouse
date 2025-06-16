"""Script to monitor data quality and generate reports."""
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List

from google.cloud import bigquery

from ..quality_monitor.monitor import DataQualityMonitor
from ..utils.logging_config import setup_logging

# Set up logging
logger = logging.getLogger(__name__)
setup_logging()

def get_historical_metrics(client: bigquery.Client, days: int = 7) -> Dict[str, Any]:
    """Get historical quality metrics from BigQuery.
    
    Args:
        client: BigQuery client
        days: Number of days of history to analyze
        
    Returns:
        Dictionary containing historical metrics
    """
    query = f"""
    WITH daily_stats AS (
        SELECT
            DATE(timestamp) as check_date,
            check_name,
            COUNT(*) as total_checks,
            COUNTIF(check_status = 'PASSED') as passed_checks,
            AVG(CAST(failed_records AS FLOAT64)/NULLIF(records_checked, 0)) as avg_failure_rate
        FROM `bgg_monitoring_dev.quality_check_results`
        WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
        GROUP BY 1, 2
    )
    SELECT
        check_date,
        check_name,
        passed_checks / total_checks as success_rate,
        avg_failure_rate
    FROM daily_stats
    ORDER BY check_date DESC, check_name
    """
    
    query_job = client.query(query)
    results = query_job.result()
    
    metrics = {
        'daily_metrics': [],
        'overall_success_rate': 0.0,
        'trend_analysis': {}
    }
    
    # Process daily metrics
    for row in results:
        metrics['daily_metrics'].append({
            'date': row.check_date.isoformat(),
            'check_name': row.check_name,
            'success_rate': row.success_rate,
            'failure_rate': row.avg_failure_rate
        })
    
    # Calculate overall success rate
    if metrics['daily_metrics']:
        total_success_rate = sum(m['success_rate'] for m in metrics['daily_metrics'])
        metrics['overall_success_rate'] = total_success_rate / len(metrics['daily_metrics'])
    
    # Analyze trends by check type
    for check_name in set(m['check_name'] for m in metrics['daily_metrics']):
        check_metrics = [m for m in metrics['daily_metrics'] if m['check_name'] == check_name]
        if check_metrics:
            latest = check_metrics[0]['success_rate']
            oldest = check_metrics[-1]['success_rate']
            trend = 'improving' if latest > oldest else 'declining' if latest < oldest else 'stable'
            metrics['trend_analysis'][check_name] = {
                'trend': trend,
                'current_rate': latest,
                'change': latest - oldest
            }
    
    return metrics

def get_api_performance_metrics(client: bigquery.Client, days: int = 7) -> Dict[str, Any]:
    """Get API performance metrics from BigQuery.
    
    Args:
        client: BigQuery client
        days: Number of days of history to analyze
        
    Returns:
        Dictionary containing API performance metrics
    """
    query = f"""
    SELECT
        DATE(timestamp) as request_date,
        COUNT(*) as total_requests,
        COUNTIF(status_code = 200) as successful_requests,
        AVG(response_time) as avg_response_time,
        AVG(retry_count) as avg_retries,
        COUNTIF(error_message IS NOT NULL) as error_count
    FROM `bgg_monitoring_dev.api_requests`
    WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
    GROUP BY request_date
    ORDER BY request_date DESC
    """
    
    query_job = client.query(query)
    results = query_job.result()
    
    metrics = {
        'daily_metrics': [],
        'overall_success_rate': 0.0,
        'avg_response_time': 0.0,
        'avg_retries': 0.0
    }
    
    total_requests = 0
    total_successful = 0
    
    for row in results:
        success_rate = row.successful_requests / row.total_requests if row.total_requests > 0 else 0
        metrics['daily_metrics'].append({
            'date': row.request_date.isoformat(),
            'total_requests': row.total_requests,
            'success_rate': success_rate,
            'avg_response_time': row.avg_response_time,
            'avg_retries': row.avg_retries,
            'error_count': row.error_count
        })
        total_requests += row.total_requests
        total_successful += row.successful_requests
    
    if total_requests > 0:
        metrics['overall_success_rate'] = total_successful / total_requests
    
    if metrics['daily_metrics']:
        metrics['avg_response_time'] = sum(m['avg_response_time'] for m in metrics['daily_metrics']) / len(metrics['daily_metrics'])
        metrics['avg_retries'] = sum(m['avg_retries'] for m in metrics['daily_metrics']) / len(metrics['daily_metrics'])
    
    return metrics

def save_report(metrics: Dict[str, Any], output_dir: str = "data/quality_reports") -> str:
    """Save quality metrics to a JSON file.
    
    Args:
        metrics: Dictionary of quality metrics
        output_dir: Directory to save reports
        
    Returns:
        Path to saved report file
    """
    # Create output directory if it doesn't exist
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"quality_report_{timestamp}.json"
    filepath = Path(output_dir) / filename
    
    # Save report
    with open(filepath, 'w') as f:
        json.dump(metrics, f, indent=2)
    
    return str(filepath)

def print_summary(metrics: Dict[str, Any]) -> None:
    """Print a human-readable summary of quality metrics.
    
    Args:
        metrics: Dictionary of quality metrics
    """
    print("\nData Quality Summary")
    print("=" * 50)
    
    # Overall metrics
    print(f"\nOverall Quality Score: {metrics['quality_metrics']['overall_success_rate']:.1%}")
    
    # Trend analysis
    print("\nTrend Analysis:")
    for check_name, trend in metrics['quality_metrics']['trend_analysis'].items():
        arrow = "↑" if trend['trend'] == 'improving' else "↓" if trend['trend'] == 'declining' else "→"
        print(f"  {check_name}: {arrow} {trend['current_rate']:.1%} ({trend['change']*100:+.1f}%)")
    
    # API Performance
    api_metrics = metrics['api_metrics']
    print("\nAPI Performance:")
    print(f"  Success Rate: {api_metrics['overall_success_rate']:.1%}")
    print(f"  Avg Response Time: {api_metrics['avg_response_time']:.2f}s")
    print(f"  Avg Retries: {api_metrics['avg_retries']:.2f}")
    
    # Recent issues
    if metrics['quality_metrics']['daily_metrics']:
        latest = metrics['quality_metrics']['daily_metrics'][0]
        if latest['failure_rate'] > 0:
            print(f"\nRecent Issues:")
            print(f"  {latest['check_name']}: {latest['failure_rate']:.1%} failure rate")

def main() -> None:
    """Run data quality monitoring checks."""
    try:
        # Initialize monitor and client
        monitor = DataQualityMonitor()
        client = bigquery.Client()
        
        # Run current quality checks
        logger.info("Running quality checks...")
        current_results = monitor.run_all_checks()
        
        # Get historical metrics
        logger.info("Fetching historical metrics...")
        quality_metrics = get_historical_metrics(client)
        api_metrics = get_api_performance_metrics(client)
        
        # Combine metrics
        metrics = {
            'timestamp': datetime.now().isoformat(),
            'current_results': current_results,
            'quality_metrics': quality_metrics,
            'api_metrics': api_metrics
        }
        
        # Save report
        report_path = save_report(metrics)
        logger.info(f"Quality report saved to: {report_path}")
        
        # Print summary
        print_summary(metrics)
        
        # Log any critical issues
        failed_checks = [name for name, passed in current_results.items() if not passed]
        if failed_checks:
            logger.warning(
                f"Failed checks: {', '.join(failed_checks)}. "
                f"Check the report at {report_path} for details."
            )
        
    except Exception as e:
        logger.error(f"Error running quality checks: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
