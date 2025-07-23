import random
import statistics
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from app.db.supabase import supabase
from app.services.nba_api_service import nba_api
from app.core.logging import logger
from app.core.cache import cached, cache
from app.utils.decorators import performance_monitor
import numpy as np

class BettingService:
    """Advanced betting service with odds calculation and simulation"""
    
    def __init__(self):
        self.min_games_for_odds = 10  # Minimum games to calculate reliable odds
        self.house_edge = 0.05  # 5% house edge
        
    @performance_monitor
    def calculate_odds(self, player_id: str, stat_key: str, threshold: float, 
                      season_year: int = 2025) -> Dict:
        """Calculate betting odds based on historical performance"""
        try:
            # Get player's historical stats for this stat
            historical_stats = self._get_player_historical_stats(
                player_id, stat_key, season_year
            )
            
            if len(historical_stats) < self.min_games_for_odds:
                return {
                    'error': f'Insufficient data. Need at least {self.min_games_for_odds} games.',
                    'games_available': len(historical_stats)
                }
            
            # Calculate probability based on historical performance
            values = [stat['stat_value'] for stat in historical_stats]
            over_count = sum(1 for value in values if value > threshold)
            under_count = len(values) - over_count
            
            # Raw probabilities
            over_probability = over_count / len(values)
            under_probability = under_count / len(values)
            
            # Adjust for recent form (last 5 games weighted more heavily)
            recent_stats = historical_stats[-5:] if len(historical_stats) >= 5 else historical_stats
            recent_over = sum(1 for stat in recent_stats if stat['stat_value'] > threshold)
            recent_probability = recent_over / len(recent_stats)
            
            # Blend historical and recent (70% historical, 30% recent)
            adjusted_over_prob = (0.7 * over_probability) + (0.3 * recent_probability)
            adjusted_under_prob = 1 - adjusted_over_prob
            
            # Apply house edge and convert to odds
            over_odds = self._probability_to_odds(adjusted_over_prob)
            under_odds = self._probability_to_odds(adjusted_under_prob)
            
            # Calculate additional metrics
            avg_value = statistics.mean(values)
            std_dev = statistics.stdev(values) if len(values) > 1 else 0
            
            return {
                'player_id': player_id,
                'stat_key': stat_key,
                'threshold': threshold,
                'odds': {
                    'over': over_odds,
                    'under': under_odds
                },
                'probabilities': {
                    'over': round(adjusted_over_prob, 3),
                    'under': round(adjusted_under_prob, 3)
                },
                'stats': {
                    'total_games': len(values),
                    'over_count': over_count,
                    'under_count': under_count,
                    'average': round(avg_value, 2),
                    'std_deviation': round(std_dev, 2),
                    'recent_form': round(recent_probability, 3)
                },
                'confidence': self._calculate_confidence(len(values), std_dev, avg_value)
            }
            
        except Exception as e:
            logger.error(f"Error calculating odds: {e}")
            return {'error': str(e)}
    
    def _probability_to_odds(self, probability: float) -> float:
        """Convert probability to decimal odds with house edge"""
        if probability <= 0:
            return 10.0  # Max odds
        if probability >= 1:
            return 1.01  # Min odds
            
        # Apply house edge
        adjusted_prob = probability * (1 + self.house_edge)
        if adjusted_prob >= 1:
            adjusted_prob = 0.99
            
        # Convert to decimal odds
        odds = 1 / adjusted_prob
        return round(odds, 2)
    
    def _calculate_confidence(self, sample_size: int, std_dev: float, avg_value: float) -> str:
        """Calculate confidence level for the odds"""
        if sample_size < 10:
            return 'Low'
        elif sample_size < 20:
            return 'Medium'
        else:
            # Factor in consistency (lower std dev = higher confidence)
            if avg_value > 0:
                coefficient_of_variation = std_dev / avg_value
                if coefficient_of_variation < 0.2:
                    return 'Very High'
                elif coefficient_of_variation < 0.4:
                    return 'High'
                else:
                    return 'Medium'
            return 'High'
    
    @cached(timeout=1800)  # Cache for 30 minutes
    def _get_player_historical_stats(self, player_id: str, stat_key: str, 
                                   season_year: int) -> List[Dict]:
        """Get player's historical stats for odds calculation"""
        try:
            result = supabase.table('player_stats').select(
                'stat_value, game_date, game_id'
            ).eq('player_id', player_id).eq('stat_key', stat_key).join(
                'seasons', 'season_id'
            ).eq('seasons.year', season_year).order(
                'game_date', desc=False
            ).execute()
            
            return result.data if result.data else []
            
        except Exception as e:
            logger.error(f"Error fetching historical stats: {e}")
            return []
    
    def place_bet(self, user_id: str, player_id: str, stat_key: str, 
                  threshold: float, side: str, stake: float = 10.0) -> Dict:
        """Place a bet with calculated odds"""
        try:
            # Calculate current odds
            odds_data = self.calculate_odds(player_id, stat_key, threshold)
            
            if 'error' in odds_data:
                return odds_data
            
            # Get the odds for the chosen side
            chosen_odds = odds_data['odds'][side]
            potential_payout = round(stake * chosen_odds, 2)
            
            # Get next game date for this player
            next_game_date = self._get_next_game_date(player_id)
            
            # Create bet record
            bet_data = {
                'user_id': user_id,
                'player_id': player_id,
                'stat_key': stat_key,
                'threshold': threshold,
                'side': side,
                'stake': stake,
                'odds': chosen_odds,
                'potential_payout': potential_payout,
                'status': 'pending',
                'game_date': next_game_date.isoformat() if next_game_date else None,
                'placed_at': datetime.utcnow().isoformat(),
                'confidence_score': self._confidence_to_score(odds_data.get('confidence', 'Medium')),
                'source': 'manual'
            }
            
            result = supabase.table('bets').insert(bet_data).execute()
            
            if result.error:
                logger.error(f"Error placing bet: {result.error.message}")
                return {'error': result.error.message}
            
            bet_id = result.data[0]['id']
            
            return {
                'success': True,
                'bet_id': bet_id,
                'bet_details': result.data[0],
                'odds_calculation': odds_data
            }
            
        except Exception as e:
            logger.error(f"Error placing bet: {e}")
            return {'error': str(e)}
    
    def _confidence_to_score(self, confidence: str) -> float:
        """Convert confidence string to numerical score"""
        confidence_map = {
            'Low': 0.3,
            'Medium': 0.5,
            'High': 0.7,
            'Very High': 0.9
        }
        return confidence_map.get(confidence, 0.5)
    
    def _get_next_game_date(self, player_id: str) -> Optional[datetime]:
        """Get the next game date for a player (simulation)"""
        # In a real app, this would fetch from NBA schedule API
        # For simulation, we'll use tomorrow
        return datetime.utcnow() + timedelta(days=1)
    
    def simulate_bet_outcome(self, bet_id: str) -> Dict:
        """Simulate bet outcome based on 'actual' performance"""
        try:
            # Get bet details
            bet_result = supabase.table('bets').select('*').eq('id', bet_id).single().execute()
            
            if bet_result.error or not bet_result.data:
                return {'error': 'Bet not found'}
            
            bet = bet_result.data
            
            if bet['status'] != 'pending':
                return {'error': 'Bet already resolved'}
            
            # Simulate player performance based on historical data
            simulated_value = self._simulate_player_performance(
                bet['player_id'], bet['stat_key']
            )
            
            # Determine outcome
            if bet['side'] == 'over':
                won = simulated_value > bet['threshold']
            else:  # under
                won = simulated_value < bet['threshold']
            
            # Calculate payout
            payout = bet['potential_payout'] if won else 0
            
            # Update bet record
            update_data = {
                'status': 'won' if won else 'lost',
                'result_value': simulated_value,
                'resolved_at': datetime.utcnow().isoformat()
            }
            
            update_result = supabase.table('bets').update(update_data).eq('id', bet_id).execute()
            
            return {
                'bet_id': bet_id,
                'outcome': 'won' if won else 'lost',
                'simulated_value': simulated_value,
                'threshold': bet['threshold'],
                'side': bet['side'],
                'stake': bet['stake'],
                'payout': payout,
                'profit_loss': payout - bet['stake']
            }
            
        except Exception as e:
            logger.error(f"Error simulating bet outcome: {e}")
            return {'error': str(e)}
    
    def _simulate_player_performance(self, player_id: str, stat_key: str) -> float:
        """Simulate realistic player performance based on historical data"""
        try:
            # Get recent historical performance
            historical_stats = self._get_player_historical_stats(player_id, stat_key, 2025)
            
            if not historical_stats:
                # Fallback to reasonable defaults by stat type
                defaults = {
                    'pts': random.uniform(15, 35),
                    'reb': random.uniform(3, 12),
                    'ast': random.uniform(2, 10),
                    'stl': random.uniform(0, 3),
                    'blk': random.uniform(0, 3)
                }
                return defaults.get(stat_key, random.uniform(5, 20))
            
            values = [stat['stat_value'] for stat in historical_stats]
            
            # Use normal distribution based on historical performance
            mean_value = statistics.mean(values)
            std_dev = statistics.stdev(values) if len(values) > 1 else mean_value * 0.2
            
            # Add some randomness but keep it realistic
            simulated = random.gauss(mean_value, std_dev)
            
            # Ensure non-negative values and reasonable bounds
            simulated = max(0, simulated)
            
            # Add some variance for realism (±20% of mean)
            variance = random.uniform(-0.2, 0.2) * mean_value
            simulated += variance
            
            return round(max(0, simulated), 1)
            
        except Exception as e:
            logger.error(f"Error simulating performance: {e}")
            return random.uniform(10, 25)  # Fallback
    
    def get_user_bets(self, user_id: str, status: str = None, limit: int = 20) -> List[Dict]:
        """Get user's betting history"""
        try:
            query = supabase.table('bets').select(
                '*, players(name, position, teams(name, abbreviation))'
            ).eq('user_id', user_id)
            
            if status:
                query = query.eq('status', status)
                
            result = query.order('placed_at', desc=True).limit(limit).execute()
            
            return result.data if result.data else []
            
        except Exception as e:
            logger.error(f"Error fetching user bets: {e}")
            return []
    
    def get_betting_statistics(self, user_id: str) -> Dict:
        """Get user's betting performance statistics"""
        try:
            bets = self.get_user_bets(user_id, limit=1000)  # Get all bets
            
            if not bets:
                return {
                    'total_bets': 0,
                    'total_wagered': 0,
                    'total_won': 0,
                    'win_rate': 0,
                    'profit_loss': 0
                }
            
            total_bets = len(bets)
            total_wagered = sum(bet['stake'] for bet in bets)
            resolved_bets = [bet for bet in bets if bet['status'] in ['won', 'lost']]
            won_bets = [bet for bet in resolved_bets if bet['status'] == 'won']
            
            total_won = sum(bet['potential_payout'] for bet in won_bets)
            win_rate = len(won_bets) / len(resolved_bets) if resolved_bets else 0
            profit_loss = total_won - sum(bet['stake'] for bet in resolved_bets)
            
            # Recent form (last 10 bets)
            recent_bets = resolved_bets[-10:] if len(resolved_bets) >= 10 else resolved_bets
            recent_wins = sum(1 for bet in recent_bets if bet['status'] == 'won')
            recent_win_rate = recent_wins / len(recent_bets) if recent_bets else 0
            
            return {
                'total_bets': total_bets,
                'resolved_bets': len(resolved_bets),
                'pending_bets': total_bets - len(resolved_bets),
                'total_wagered': round(total_wagered, 2),
                'total_won': round(total_won, 2),
                'win_rate': round(win_rate * 100, 1),
                'recent_win_rate': round(recent_win_rate * 100, 1),
                'profit_loss': round(profit_loss, 2),
                'roi': round((profit_loss / total_wagered * 100), 1) if total_wagered > 0 else 0,
                'best_stat': self._get_best_performing_stat(won_bets),
                'favorite_stat': self._get_most_bet_stat(bets)
            }
            
        except Exception as e:
            logger.error(f"Error calculating betting statistics: {e}")
            return {'error': str(e)}
    
    def _get_best_performing_stat(self, won_bets: List[Dict]) -> str:
        """Find the stat type with highest win rate"""
        if not won_bets:
            return 'N/A'
            
        stat_counts = {}
        for bet in won_bets:
            stat_key = bet['stat_key']
            stat_counts[stat_key] = stat_counts.get(stat_key, 0) + 1
            
        return max(stat_counts.items(), key=lambda x: x[1])[0] if stat_counts else 'N/A'
    
    def _get_most_bet_stat(self, bets: List[Dict]) -> str:
        """Find the most frequently bet stat type"""
        if not bets:
            return 'N/A'
            
        stat_counts = {}
        for bet in bets:
            stat_key = bet['stat_key']
            stat_counts[stat_key] = stat_counts.get(stat_key, 0) + 1
            
        return max(stat_counts.items(), key=lambda x: x[1])[0] if stat_counts else 'N/A'
    
    def auto_resolve_pending_bets(self) -> Dict:
        """Automatically resolve pending bets (for simulation)"""
        try:
            # Get all pending bets
            pending_result = supabase.table('bets').select('*').eq('status', 'pending').execute()
            
            if not pending_result.data:
                return {'resolved': 0, 'message': 'No pending bets to resolve'}
            
            resolved_count = 0
            results = []
            
            for bet in pending_result.data:
                # Simulate outcome for each bet
                outcome = self.simulate_bet_outcome(bet['id'])
                if 'error' not in outcome:
                    resolved_count += 1
                    results.append(outcome)
            
            return {
                'resolved': resolved_count,
                'total_pending': len(pending_result.data),
                'results': results
            }
            
        except Exception as e:
            logger.error(f"Error auto-resolving bets: {e}")
            return {'error': str(e)}

# Global instance
betting_service = BettingService()