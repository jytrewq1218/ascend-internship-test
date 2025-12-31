# Ascend Internship Test

## 1. 가장 위험한 불확실성은 무엇이었는가?

가장 위험한 불확실성은 가격 판단에 사용되는 여러 스트림 간의 유기적 관계가 유지되지 않는 상황입니다.
단일 스트림의 오류나 지연 자체보다 다른 스트림이 그럴듯해 보이지만 서로 모순된 가격 상태를 만드는 것이 치명적이라고 생각했습니다.
• Orderbook, Trade, Ticker, Liquidation 간 가격 합의가 붕괴되는 경우
• 일부 스트림은 정상적으로 보이지만, 다른 스트림이 지연 및 누락되어 동일 시점의 시장 상태를 설명하지 못하는 경우
• 대규모 청산 직후 체결 가격, 호가 구조, 지표 가격이 서로 다른 방향으로 움직이는 구간
이러한 상황에서는 개별 이벤트의 데이터 정합성이나 단일 스트림 품질이 양호하더라도 판단 불가능한 상태로 보고 스트림 간 관계가 유지되고 있는지를 가설로 검증하는 구조를 채택했습니다.

---

## 2. Dirty Data로부터 어떤 판단 조건을 정의했는가?

Dirty Data는 DataTrust 단계에서 정량 조건으로 정의했습니다.
판단 대상은 일반적인 마켓에서 형성되지 않는 데이터의 특징입니다.

1. 이벤트 품질 기반 조건

- out-of-order / 지연 이벤트 비율
- forced flush 발생 횟수
- align buffer 과도 증가 (이벤트 누락 신호)
- Sanitization의 QUARANTINE 비율 상승

2. 가격 구조 기반 조건

- fat-finger: Trade 가격과 Orderbook mid 가격 간 과도한 괴리
- crossed market: Orderbook bid ≥ ask
- spread explosion: Orderbook spread bps 급증
- trade jump: 직전 trade 대비 급격한 가격 점프

이 조건들은 cfg에서 DEGRADED / UNTRUSTED로 각각 수치값 조건을 걸어 평가에 활용됩니다.

---

## 3. 그 조건이 가설(의사결정 구조)에 어떻게 반영되는가?

Dirty Data의 조건들이 가설에 직접적으로 반영되지 않습니다.
조건들은 Data Trust 단계에서 판단에 활용되고 신뢰 가능한 데이터인 경우 가설 검증으로 이어질 수 있도록 설계했습니다.
마지막 Decision 단계에서 추가로 판단된 가설과 결합하여 전체 행동을 결정합니다.
즉, Dirty Data는 가설을 오염시키지 않고 가설 자체를 제한하거나 무효화하는 방식으로 반영했습니다.

---

## 4. 가설 변화가 시스템 동작을 어떻게 바꾸는가?

가설에서는 여러 스트림 간의 유기적 관계가 깨지는지를 보고 있습니다.
따라서 가격 간 괴리폭으로 정의한 스트림 간의 관계가 조금이라도 어그러지기 시작하면 판단에 제한이 걸립니다.
임계치를 초과하기 시작하면 판단이 중단됩니다.
관계가 회복되고 어느정도 지속이 된다면 다시 판단 허용으로 되돌림으로써 안정성 지속 여부에 초점을 두었습니다.

---

## 5. 언제 판단을 중단하도록 설계했는가?

1. 판단 중단 (HALTED)

다음 조건 중 하나라도 만족하면 판단을 즉시 중단합니다.

(A) DataTrust = UNTRUSTED
Dirty Data가 구조적으로 누적되거나 시장 미시구조가 붕괴된 경우
• Sanitization 기반
• QUARANTINE 비율이 임계치 초과
(quarantine_untrusted_rate)
• Time Alignment 기반
• late event 비율 과도
(late_untrusted_rate)
• forced flush 횟수 초과
(forced_flush_untrusted_count)
• alignment buffer 크기 과도
(buffer_len_untrusted)
• Orderbook 기반 Dirty Data
• crossed market 발생
(best_bid >= best_ask)
• Trade ↔ Orderbook 불일치
• trade 가격이 orderbook mid 대비 fat-finger 수준 괴리
(fat_finger_untrusted_bps)

(B) Hypothesis = INVALID
시장 스트림 간 가격 합의(consensus)가 붕괴된 경우
• 다음 스트림들의 가격을 종합 비교
• Trades
• Liquidations
• Orderbook mid
• Ticker(mark / index / last)
• 최악 괴리(bps)가 임계치 초과
(invalid_price_diverge_bps)
• 또는 안정화 타이머가 리셋될 만큼 구조적 붕괴 발생

2. 판단 제한 (RESTRICTED)

다음 조건에서는 판단을 중단하지는 않지만 제한합니다.

(A) DataTrust = DEGRADED
Dirty Data의 초기 신호 또는 품질 저하가 감지된 상태
• Sanitization
• 단일 QUARANTINE 이벤트 발생
• Time Alignment
• late rate 경고 수준 초과
(late_degraded_rate)
• forced flush 발생
(forced_flush_degraded_count)
• buffer_len 경고 수준 초과
(buffer_len_degraded)
• Orderbook 품질 저하
• spread 폭발
(spread_explode_bps)
• Trade 이상 징후
• trade와 orderbook mid 괴리 (fat-finger 경고)
(fat_finger_degraded_bps)
• 직전 trade 대비 급격한 가격 점프
(trade_jump_degraded_bps)
• 이벤트 이상
• out-of-order timestamp
• duplicate event

(B) Hypothesis = WEAKENING
시장 구조는 유지되고 있으나 가격 합의가 약화된 상태
• 스트림 간 가격 괴리 발생
• 괴리 수준이 invalid 미만, weak 이상
(weak_price_diverge_bps)

(C) Hypothesis 안정화 대기 상태 (Stabilizing)
가격 합의는 정상 범위이나 충분한 시간 동안 유지되지 않은 경우
• consensus OK 상태가 유지되었으나
• 안정화 최소 시간 미충족
(stable_min_duration_ms)

---

## 6. 지금 다시 설계한다면, 가장 먼저 제거하거나 단순화할 요소는 무엇인가?

신속한 의사결정이 중요한 HFT에서 지연은 치명적인 오류사항입니다.
멀티스레딩, 소켓 커넥션 등의 영향으로 이벤트들이 순차적으로 들어오지 않음을 고려해
time aligner에서 버퍼를 두어 일정시간 잠시 대기하며 이벤트를 재정렬하였습니다.
결국 버퍼 대기 시간만큼의 전략에 지연이 생기기에 이를 제거하고 싶습니다.
일정 시간 동안 대기하며 이벤트 보관이 아닌 일단 바로 반영하되 일정 시간 내의 이벤트는 복구하는 식으로 변경할 수 있지 않을까 생각합니다.
