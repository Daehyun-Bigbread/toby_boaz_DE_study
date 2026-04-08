import json
import os
import time
from kafka import KafkaConsumer
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.live import Live
from rich.layout import Layout

# 전역 설정
MAX_BOSS_HP = 1000000
BOOTSTRAP_SERVERS = ['kafka:9092']
TOPIC_NAME = 'boss-attacks'
GROUP_ID = 'dps-monitor-group-v4'

console = Console()

def create_dashboard(boss_hp, user_dps, last_attack, current_offset):
    """실시간 대시보드 레이아웃 생성 (안정화 버전)"""
    layout = Layout()
    
    # 1. 상단: 보스 체력 상태
    hp_percent = max(0, (boss_hp / MAX_BOSS_HP) * 100)
    hp_color = "green" if hp_percent > 50 else "yellow" if hp_percent > 20 else "red"
    hp_bar = "█" * int(hp_percent // 5) + "░" * (20 - int(hp_percent // 5))
    
    boss_panel = Panel(
        f"[{hp_color}]{hp_bar}[/] {boss_hp:,} / {MAX_BOSS_HP:,} ({hp_percent:.1f}%)\n"
        f"[white]마지막 공격: {last_attack if last_attack else '대기 중...'}[/]",
        title="🔥 [bold red]RAID BOSS: BALROG[/]",
        border_style="red"
    )
    
    # 2. 중앙: 랭킹 테이블
    table = Table(expand=True)
    table.add_column("Rank", style="dim", width=6)
    table.add_column("User ID", style="cyan")
    table.add_column("Total Damage", justify="right", style="bold yellow")
    table.add_column("Contribution", justify="right", style="green")

    sorted_dps = sorted(user_dps.items(), key=lambda x: x[1], reverse=True)
    for i, (user, damage) in enumerate(sorted_dps[:10], 1):
        user_display = f"👑 [bold]{user}[/]" if i == 1 else user
        total_dmg_dealt = (MAX_BOSS_HP - boss_hp)
        contrib = (damage / total_dmg_dealt) * 100 if total_dmg_dealt > 0 else 0
        table.add_row(str(i), user_display, f"{damage:,}", f"{contrib:.1f}%")

    # 3. 하단: 정보 바 (기존 split_footer 대신 split_column 사용)
    footer_panel = Panel(
        f"[dim]Kafka Offset: {current_offset} | Group: {GROUP_ID}[/]", 
        style="blue"
    )

    # 레이아웃 나누기 (전체 화면을 3개로 분할)
    layout.split_column(
        Layout(boss_panel, size=6, name="header"),
        Layout(table, name="body"),
        Layout(footer_panel, size=3, name="footer")
    )
    
    return layout

def main():
    # 카프카 컨슈머 설정
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset='earliest',
        enable_auto_commit=False,  # ★ 수동 오프셋 커밋 설정
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    boss_hp = MAX_BOSS_HP
    user_dps = {}
    last_attack_info = ""
    current_offset = 0

    console.print("[bold green][INFO][/] 카프카 보스 레이드 모니터링을 시작합니다...")
    
    # Live Display 시작
    with Live(create_dashboard(boss_hp, user_dps, last_attack_info, current_offset), refresh_per_second=4) as live:
        try:
            for message in consumer:
                attack = message.value
                user = attack['user_id']
                damage = attack['damage']
                current_offset = message.offset

                # 대미지 처리 로직
                boss_hp -= damage
                user_dps[user] = user_dps.get(user, 0) + damage
                last_attack_info = f"{user}님이 {attack['skill']}로 {damage:,} 대미지!"

                # 화면 업데이트
                live.update(create_dashboard(boss_hp, user_dps, last_attack_info, current_offset))

                # ★ 수동 오프셋 커밋 (메시지 처리 완료 후 카프카에 보고)
                # 이 코드를 주석처리하고 껐다 켜면, 이전에 처리한 데이터를 다시 읽어오는 걸 볼 수 있습니다.
                consumer.commit()

        except KeyboardInterrupt:
            console.print("\n[bold yellow][INFO][/] 사용자에 의해 모니터링이 중단되었습니다.")
        finally:
            consumer.close()

if __name__ == "__main__":
    main()