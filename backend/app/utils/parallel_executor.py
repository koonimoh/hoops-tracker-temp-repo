import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from typing import List, Callable, Any, Dict, Optional
from app.core.config import settings
from app.core.logging import logger

class ParallelExecutor:
    """Utility class for parallel execution of tasks"""
    
    def __init__(self):
        self.thread_pool = ThreadPoolExecutor(max_workers=settings.thread_pool_size)
        self.process_pool = ProcessPoolExecutor(max_workers=settings.max_workers)
    
    def execute_in_threads(self, func: Callable, items: List[Any], **kwargs) -> List[Any]:
        """Execute function for each item in separate threads"""
        results = []
        
        # Submit all tasks
        future_to_item = {
            self.thread_pool.submit(func, item, **kwargs): item 
            for item in items
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_item):
            item = future_to_item[future]
            try:
                result = future.result()
                results.append({
                    'item': item,
                    'result': result,
                    'success': True
                })
            except Exception as e:
                logger.error(f"Error processing item {item}: {e}")
                results.append({
                    'item': item,
                    'result': None,
                    'success': False,
                    'error': str(e)
                })
        
        return results
    
    def execute_in_processes(self, func: Callable, items: List[Any], **kwargs) -> List[Any]:
        """Execute function for each item in separate processes"""
        results = []
        
        # Submit all tasks
        future_to_item = {
            self.process_pool.submit(func, item, **kwargs): item 
            for item in items
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_item):
            item = future_to_item[future]
            try:
                result = future.result()
                results.append({
                    'item': item,
                    'result': result,
                    'success': True
                })
            except Exception as e:
                logger.error(f"Error processing item {item}: {e}")
                results.append({
                    'item': item,
                    'result': None,
                    'success': False,
                    'error': str(e)
                })
        
        return results
    
    async def execute_async_batch(self, async_func: Callable, items: List[Any], batch_size: int = 10) -> List[Any]:
        """Execute async function in batches"""
        results = []
        
        # Process items in batches to avoid overwhelming the system
        for i in range(0, len(items), batch_size):
            batch = items[i:i + batch_size]
            
            # Create tasks for this batch
            tasks = [async_func(item) for item in batch]
            
            # Execute batch
            try:
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for item, result in zip(batch, batch_results):
                    if isinstance(result, Exception):
                        logger.error(f"Error processing item {item}: {result}")
                        results.append({
                            'item': item,
                            'result': None,
                            'success': False,
                            'error': str(result)
                        })
                    else:
                        results.append({
                            'item': item,
                            'result': result,
                            'success': True
                        })
            except Exception as e:
                logger.error(f"Error processing batch: {e}")
                for item in batch:
                    results.append({
                        'item': item,
                        'result': None,
                        'success': False,
                        'error': str(e)
                    })
        
        return results
    
    def cleanup(self):
        """Cleanup executors"""
        self.thread_pool.shutdown(wait=True)
        self.process_pool.shutdown(wait=True)

# Global instance
parallel_executor = ParallelExecutor()

def parallelize_io_bound(func: Callable, items: List[Any], **kwargs) -> List[Any]:
    """Helper function for I/O bound parallel execution"""
    return parallel_executor.execute_in_threads(func, items, **kwargs)

def parallelize_cpu_bound(func: Callable, items: List[Any], **kwargs) -> List[Any]:
    """Helper function for CPU bound parallel execution"""
    return parallel_executor.execute_in_processes(func, items, **kwargs)