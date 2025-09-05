import json
import os
from pathlib import Path
import pandas as pd
from collections import defaultdict
import re
from typing import Dict, List, Set, Tuple
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import numpy as np
from wordcloud import WordCloud
import networkx as nx
from matplotlib.colors import ListedColormap

# Define error types and patterns
class ErrorAnalyzer:
    def __init__(self, debug_dir: Path):
        self.debug_dir = debug_dir
        self.js_error_dir = debug_dir / "js_errors"
        self.design_blocker_dir = debug_dir / "design_blockers"
        self.error_patterns = {
            'js': {
                'timeout': r'timeout|deadline|exceeded|deadline',
                'network': r'network|connection|failed|offline',
                'syntax': r'syntax|parse|error|invalid',
                'framework': r'react|vue|angular|jquery',
                'security': r'cross-origin|cors|security|policy',
                'resource': r'resource|load|fetch|download',
                'api': r'api|endpoint|fetch|axios|xhr',
                'memory': r'memory|heap|allocation|out of',
                'performance': r'performance|slow|lag|delay',
            },
            'design': {
                'dynamic_content': r'dynamic|async|lazy|loading',
what 
        self.resolution_suggestions = {
            'timeout': [
                "Increase timeout duration",
                "Add retry logic with exponential backoff",
                "Implement connection pooling"
            ],
            'network': [
                "Check internet connectivity",
                "Use proxy server",
                "Implement retry with different network interfaces"
            ],
            'syntax': [
                "Validate JavaScript code",
                "Check for missing semicolons",
                "Verify correct syntax"
            ],
            'framework': [
                "Check framework version compatibility",
                "Update framework dependencies",
                "Use framework-specific error handling"
            ],
            'security': [
                "Implement CORS headers",
                "Use secure protocols",
                "Add security headers"
            ],
            'resource': [
                "Preload critical resources",
                "Implement lazy loading",
                "Optimize resource loading"
            ],
            'api': [
                "Add API error handling",
                "Implement rate limiting",
                "Add retry logic for API calls"
            ],
            'memory': [
                "Optimize memory usage",
                "Implement garbage collection",
                "Use memory profiling"
            ],
            'performance': [
                "Optimize JavaScript execution",
                "Implement caching",
                "Use performance monitoring"
            ],
            'dynamic_content': [
                "Wait for content to load",
                "Implement polling",
                "Use event listeners"
            ],
            'loading': [
                "Add loading indicators",
                "Implement timeout handling",
                "Use loading states"
            ],
            'interaction': [
                "Add event handlers",
                "Implement user feedback",
                "Use proper event delegation"
            ],
            'css': [
                "Check CSS specificity",
                "Use proper CSS selectors",
                "Implement CSS fallbacks"
            ]
        }
        
        self.error_counts = defaultdict(int)
        self.error_patterns_found = defaultdict(set)
        self.error_correlations = defaultdict(lambda: defaultdict(int))
        
    def analyze_js_errors(self) -> Dict:
        """Analyze JavaScript errors"""
        js_errors = []
        
        for error_file in self.js_error_dir.glob("*.json"):
            try:
                with open(error_file, 'r', encoding='utf-8') as f:
                    error_data = json.load(f)
                    
                # Categorize error
                error_type = self._categorize_error(error_data['error_details']['message'], 'js')
                self.error_counts[error_type] += 1
                
                # Track patterns
                patterns = self._find_patterns(error_data['error_details']['message'], 'js')
                for pattern in patterns:
                    self.error_patterns_found[error_type].add(pattern)
                    
                # Track correlations
                for pattern1 in patterns:
                    for pattern2 in patterns:
                        if pattern1 != pattern2:
                            self.error_correlations[pattern1][pattern2] += 1
                            self.error_correlations[pattern2][pattern1] += 1
                    
                js_errors.append({
                    'timestamp': error_data['timestamp'],
                    'company': error_data['company'],
                    'error_type': error_type,
                    'patterns': list(patterns),
                    'suggestions': self.get_resolution_suggestions(error_type)
                })
                
            except Exception as e:
                print(f"Error analyzing {error_file}: {e}")
                continue
        
        return js_errors
        
    def analyze_design_blockers(self) -> Dict:
        """Analyze design-related blockers"""
        design_blockers = []
        
        for error_file in self.design_blocker_dir.glob("*.json"):
            try:
                with open(error_file, 'r', encoding='utf-8') as f:
                    error_data = json.load(f)
                    
                # Categorize error
                error_type = self._categorize_error(error_data['error_details']['message'], 'design')
                self.error_counts[error_type] += 1
                
                # Track patterns
                patterns = self._find_patterns(error_data['error_details']['message'], 'design')
                for pattern in patterns:
                    self.error_patterns_found[error_type].add(pattern)
                    
                # Track correlations
                for pattern1 in patterns:
                    for pattern2 in patterns:
                        if pattern1 != pattern2:
                            self.error_correlations[pattern1][pattern2] += 1
                            self.error_correlations[pattern2][pattern1] += 1
                    
                design_blockers.append({
                    'timestamp': error_data['timestamp'],
                    'company': error_data['company'],
                    'error_type': error_type,
                    'patterns': list(patterns),
                    'suggestions': self.get_resolution_suggestions(error_type)
                })
                
            except Exception as e:
                print(f"Error analyzing {error_file}: {e}")
                continue
        
        return design_blockers
        
    def _categorize_error(self, error_message: str, error_type: str) -> str:
        """Categorize error based on message"""
        patterns = self.error_patterns[error_type]
        for category, pattern in patterns.items():
            if re.search(pattern, error_message.lower()):
                return category
        return 'other'
        
    def _find_patterns(self, error_message: str, error_type: str) -> Set[str]:
        """Find matching patterns in error message"""
        patterns = self.error_patterns[error_type]
        found = set()
        
        for category, pattern in patterns.items():
            if re.search(pattern, error_message.lower()):
                found.add(category)
        
        return found
        
    def get_resolution_suggestions(self, error_type: str) -> List[str]:
        """Get resolution suggestions for an error type"""
        suggestions = self.resolution_suggestions.get(error_type, [])
        if not suggestions:
            suggestions = self.resolution_suggestions['other']
        return suggestions
        
    def generate_analysis_report(self) -> Dict:
        """Generate comprehensive error analysis report"""
        js_errors = self.analyze_js_errors()
        design_blockers = self.analyze_design_blockers()
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'total_errors': sum(self.error_counts.values()),
            'error_types': dict(self.error_counts),
            'patterns_found': {
                error_type: list(patterns)
                for error_type, patterns in self.error_patterns_found.items()
            },
            'error_correlations': dict(self.error_correlations),
            'js_errors': js_errors,
            'design_blockers': design_blockers
        }
        
        # Generate visualizations
        self._generate_visualizations()
        
        return report
        
    def _generate_visualizations(self):
        """Generate enhanced visualizations"""
        plt.style.use('seaborn')
        
        # Error Type Distribution with Correlations
        plt.figure(figsize=(12, 8))
        error_types = list(self.error_counts.keys())
        counts = [self.error_counts[etype] for etype in error_types]
        
        # Create bar plot
        sns.barplot(x=error_types, y=counts)
        plt.title('Error Type Distribution')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(self.debug_dir / 'error_type_distribution.png')
        plt.close()
        
        # Pattern Frequency with Correlations
        pattern_counts = defaultdict(int)
        for patterns in self.error_patterns_found.values():
            for pattern in patterns:
                pattern_counts[pattern] += 1
                
        # Create correlation matrix
        pattern_list = list(pattern_counts.keys())
        correlation_matrix = np.zeros((len(pattern_list), len(pattern_list)))
        
        for i, pattern1 in enumerate(pattern_list):
            for j, pattern2 in enumerate(pattern_list):
                correlation_matrix[i][j] = self.error_correlations[pattern1][pattern2]
        
        plt.figure(figsize=(12, 8))
        sns.heatmap(correlation_matrix, 
                   annot=True,
                   xticklabels=pattern_list,
                   yticklabels=pattern_list,
                   cmap='coolwarm')
        plt.title('Pattern Correlation Matrix')
        plt.tight_layout()
        plt.savefig(self.debug_dir / 'pattern_correlations.png')
        plt.close()
        
        # Error Type Word Cloud
        text = " ".join(error_types * counts)
        wordcloud = WordCloud(width=800, height=400, background_color='white').generate(text)
        
        plt.figure(figsize=(12, 8))
        plt.imshow(wordcloud, interpolation='bilinear')
        plt.axis('off')
        plt.title('Error Type Word Cloud')
        plt.tight_layout()
        plt.savefig(self.debug_dir / 'error_type_wordcloud.png')
        plt.close()
        
        # Error Network Graph
        plt.figure(figsize=(12, 8))
        G = nx.Graph()
        
        # Add nodes
        for error_type in error_types:
            G.add_node(error_type)
            
        # Add edges based on correlations
        for pattern1 in self.error_correlations:
            for pattern2 in self.error_correlations[pattern1]:
                weight = self.error_correlations[pattern1][pattern2]
                if weight > 0:
                    G.add_edge(pattern1, pattern2, weight=weight)
                    
        # Draw network graph
        pos = nx.spring_layout(G)
        weights = [G[u][v]['weight'] for u,v in G.edges()]
        
        nx.draw(G, pos,
                node_color='lightblue',
                node_size=1000,
                edge_color=weights,
                width=[w/10 for w in weights],
                with_labels=True)
                
        plt.title('Error Network Graph')
        plt.tight_layout()
        plt.savefig(self.debug_dir / 'error_network.png')
        plt.close()
        
        # Time Series Analysis
        plt.figure(figsize=(12, 6))
        
        # Create time series data
        timestamps = []
        for error in js_errors + design_blockers:
            timestamps.append(datetime.fromisoformat(error['timestamp']))
            
        # Create histogram
        plt.hist(timestamps, bins=20)
        plt.title('Error Time Distribution')
        plt.xlabel('Time')
        plt.ylabel('Number of Errors')
        plt.tight_layout()
        plt.savefig(self.debug_dir / 'error_time_distribution.png')
        plt.close()
        
        # Company Error Distribution
        companies = defaultdict(int)
        for error in js_errors + design_blockers:
            companies[error['company']] += 1
            
        plt.figure(figsize=(12, 6))
        sns.barplot(x=list(companies.keys()), y=list(companies.values()))
        plt.title('Company Error Distribution')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(self.debug_dir / 'company_error_distribution.png')
        plt.close()
        
    def save_report(self, report: Dict):
        """Save enhanced analysis report"""
        report_path = self.debug_dir / 'error_analysis_report.json'
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2)
            
        # Save suggestions separately
        suggestions_path = self.debug_dir / 'resolution_suggestions.json'
        with open(suggestions_path, 'w', encoding='utf-8') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'suggestions': self.resolution_suggestions,
                'error_correlations': dict(self.error_correlations)
            }, f, indent=2)
            
    def analyze_website_patterns(self, html_content: str) -> Dict:
        """Enhanced website pattern analysis"""
        patterns = {
            'framework': {
                'react': r'react|create-react-app',
                'vue': r'vue|nuxt',
                'angular': r'angular|ng-',
                'jquery': r'jquery',
                'svelte': r'svelte',
                'ember': r'ember',
            },
            'dynamic': {
                'spa': r'single page application|spa',
                'lazy_load': r'lazy|loading|async',
                'api': r'api|endpoint|fetch|axios',
                'web_components': r'web components|custom elements',
                'progressive_web_app': r'progressive web app|pwa',
            },
            'security': {
                'anti-bot': r'bot|captcha|anti-bot',
                'protection': r'cloudflare|akamai|fingerprint',
                'ddos_protection': r'ddos|attack|protection',
                'rate_limiting': r'rate limiting|throttle',
            },
            'performance': {
                'lazy_loading': r'lazy|loading|async',
                'image_optimization': r'image optimization|webp',
                'cdn_usage': r'cdn|cloudfront|akamai',
                'caching': r'cache|caching|storage',
            }
        }
        
        found_patterns = defaultdict(set)
        
        for category, sub_patterns in patterns.items():
            for name, pattern in sub_patterns.items():
                if re.search(pattern, html_content.lower()):
                    found_patterns[category].add(name)
        
        return found_patterns
        
    def visualize_website_patterns(self, patterns: Dict):
        """Enhanced visualization of website patterns"""
        # Create multiple visualizations
        
        # Heatmap
        plt.figure(figsize=(12, 6))
        
        # Create data for visualization
        data = []
        for category, sub_patterns in patterns.items():
            for pattern in sub_patterns:
                data.append({
                    'Category': category,
                    'Pattern': pattern,
                    'Count': 1  # Since we're just showing presence
                })
        
        df = pd.DataFrame(data)
        
        # Create heatmap
        heatmap = pd.crosstab(df['Category'], df['Pattern'])
        plt.figure(figsize=(12, 8))
        sns.heatmap(heatmap, annot=True, cmap='YlGnBu')
        plt.title('Website Pattern Analysis')
        plt.tight_layout()
        plt.savefig(self.debug_dir / 'website_patterns.png')
        plt.close()
        
        # Pattern Distribution
        plt.figure(figsize=(12, 6))
        
        # Count patterns per category
        category_counts = defaultdict(int)
        for category, sub_patterns in patterns.items():
            category_counts[category] = len(sub_patterns)
            
        sns.barplot(x=list(category_counts.keys()), y=list(category_counts.values()))
        plt.title('Pattern Distribution by Category')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(self.debug_dir / 'pattern_distribution.png')
        plt.close()
        
        # Pattern Correlation Network
        plt.figure(figsize=(12, 8))
        G = nx.Graph()
        
        # Add nodes
        for category in patterns:
            G.add_node(category)
            
        # Add edges based on pattern relationships
        for category1 in patterns:
            for category2 in patterns:
                if category1 != category2:
                    # Calculate similarity based on shared patterns
                    shared = len(patterns[category1].intersection(patterns[category2]))
                    if shared > 0:
                        G.add_edge(category1, category2, weight=shared)
                        
        # Draw network graph
        pos = nx.spring_layout(G)
        weights = [G[u][v]['weight'] for u,v in G.edges()]
        
        nx.draw(G, pos,
                node_color='lightblue',
                node_size=1000,
                edge_color=weights,
                width=[w/2 for w in weights],
                with_labels=True)
                
        plt.title('Pattern Correlation Network')
        plt.tight_layout()
        plt.savefig(self.debug_dir / 'pattern_network.png')
        plt.close()
