from pathlib import Path
from typing import Any, List, Optional, Dict, Tuple
import time
import re
from collections import defaultdict

from endstone.plugin import Plugin
from endstone.event import event_handler, ActorDeathEvent, ActorSpawnEvent
from endstone.actor import Mob
from endstone.command import Command, CommandSender
from endstone import Player

# Optional interact events (feed-to-pop)
try:
    from endstone.event import PlayerInteractActorEvent
    HAS_INTERACT_ACTOR = True
except Exception:
    PlayerInteractActorEvent = None  # type: ignore
    HAS_INTERACT_ACTOR = False

try:
    from endstone.event import PlayerInteractEntityEvent  # type: ignore
    HAS_INTERACT_ENTITY = True
except Exception:
    PlayerInteractEntityEvent = None  # type: ignore
    HAS_INTERACT_ENTITY = False

# Optional silent wrapper
try:
    from endstone.command import CommandSenderWrapper  # type: ignore
    HAS_CMD_WRAPPER = True
except Exception:
    CommandSenderWrapper = None  # type: ignore
    HAS_CMD_WRAPPER = False

# Optional ActorHurtEvent (strict-lethal pre-death decrement)
try:
    from endstone.event import ActorHurtEvent
    HAS_HURT = True
except Exception:
    ActorHurtEvent = None  # type: ignore
    HAS_HURT = False

# UI
from .menuui import MobstackerMenu

STACK_TAG = "mobstack:leader"
NAME_FORMAT = "×{count}"
SIGNATURE = "\u00A0\uFEFF"  # NBSP + BOM
MAX_FASTPAIR_CHECKS = 3000

ID_ALIASES = {
    "zombie_piglin": "minecraft:zombified_piglin",
    "minecraft:zombie_piglin": "minecraft:zombified_piglin",
    "minecraft:zombified_piglin": "minecraft:zombified_piglin",
    "minecraft:zombie_pigman": "minecraft:zombified_piglin",
    "zombie_pigman": "minecraft:zombified_piglin",
}

BREED_ITEMS: dict[str, set[str]] = {
    "minecraft:cow": {"minecraft:wheat"},
    "minecraft:sheep": {"minecraft:wheat"},
    "minecraft:pig": {"minecraft:wheat", "minecraft:carrot", "minecraft:potato", "minecraft:beetroot"},
    "minecraft:chicken": {"minecraft:wheat_seeds", "minecraft:beetroot_seeds", "minecraft:pumpkin_seeds", "minecraft:melon_seeds"},
}


class MobStacker(Plugin):
    api_version = "0.10"

    commands = {
        "mobstackui": {
            "description": "Open MobStacker settings UI.",
            "usages": ["/mobstackui"],
            "permissions": ["mobstacker.command.mobstackui"],
        },
    }
    permissions = {
        "mobstacker.command.mobstackui": {"description": "Use /mobstackui", "default": "op"},
    }

    # ---------- lifecycle ----------
    def on_load(self) -> None:
        self._cfg: dict[str, Any] = {}
        self._cfg_mtime: Optional[float] = None
        self._allowed_cache: set[str] = set()
        self._last_feed_pop: dict[int, int] = {}
        self._breed_cooldown_until: dict[int, int] = {}
        self._silent = None

        self._counts: dict[int, int] = {}  # runtime_id -> count
        self._pending: Dict[Tuple[str, int, int, int, str], int] = {}
        self._death_handled_at: dict[int, int] = {}

        self._load_or_create_config()
        self._rebuild_allowed_cache()

    def on_enable(self) -> None:
        self.register_events(self)
        self._schedule_scan()
        s = self._s()

        if bool(s.get("silence_command_feedback", True)):
            self._run_cmd("gamerule sendcommandfeedback false")
            self._run_cmd("gamerule commandblockoutput false")

        self._reindex_from_names()

        if not self._quiet():
            self.logger.info(
                f"MobStacker enabled (radius={s['radius']}, min_group={s['min_group']}, "
                f"max_stack={s['max_stack_size']}; {len(s['allowed_types'])} allowed types)"
            )
            if not (HAS_INTERACT_ACTOR or HAS_INTERACT_ENTITY) or not s.get("feed_pop_enabled", True):
                self.logger.info("MobStacker: feed-pop disabled (no interact event or config disabled).")

    def on_disable(self) -> None:
        try:
            self.server.scheduler.cancel_tasks(self)
        except Exception:
            pass

    # ---------- command ----------
    def on_command(self, sender: CommandSender, command: Command, args: List[str]) -> bool:
        try:
            if (command.name or "").lower() != "mobstackui":
                return False
            p = sender if isinstance(sender, Player) else None
            if not p:
                sender.send_message("§7This command is player-only.")
                return True
            if not self._is_op_or_perm(p, "mobstacker.command.mobstackui"):
                p.send_message("§cYou do not have permission.")
                return True
            MobstackerMenu(self).open_main(p)
            return True
        except Exception as e:
            try: sender.send_message("§cFailed to open MobStacker UI. Check logs.")
            except Exception: pass
            self.logger.error(f"/mobstackui error: {e}")
            return True

    # ---------- helpers ----------
    def _is_op_or_perm(self, p: Player, perm: str) -> bool:
        for attr in ("is_op", "isOp"):
            if hasattr(p, attr):
                v = getattr(p, attr)
                try:
                    if (v() if callable(v) else bool(v)): return True
                except Exception:
                    pass
        for meth in ("has_permission", "hasPermission", "check_permission"):
            if hasattr(p, meth):
                try:
                    if getattr(p, meth)(perm): return True
                except Exception:
                    pass
        return False

    @staticmethod
    def _normalize_id(raw: str) -> str:
        t = (raw or "").strip().lower()
        if not t: return t
        if ":" not in t: t = "minecraft:" + t
        return ID_ALIASES.get(t, t)

    def _same_type(self, a: Mob, b: Mob) -> bool:
        try:
            return self._normalize_id(a.type) == self._normalize_id(b.type)
        except Exception:
            return False

    # ================= adult-only patch =================
    def _force_adult_replace(self, a: Mob) -> None:
        try:
            s = self._s()
            if not self._is_allowed(a.type, s): return
            if not self._is_baby(a): return
            etype = self._normalize_id(a.type)
            loc = a.location
            dim_name = self._dim_token(a.dimension)
            bx, by, bz = self._block_center(loc.x, loc.y, loc.z)
            try:
                self._counts.pop(int(a.runtime_id), None)
                a.remove()
            except Exception:
                pass
            self._safe_summon(etype, dim_name, bx, by, bz)
        except Exception:
            pass

    def _adultize_and_return_sameblock(self, etype: str, dim_name: str, bx: float, by: float, bz: float, cand: Optional[Mob]):
        if not cand: return None
        try:
            if not self._is_baby(cand):
                return cand
            pre_ids = self._snapshot_same_block_ids(etype, dim_name, bx, by, bz)
            self._force_adult_replace(cand)
            adult = self._find_newborn_by_diff(etype, dim_name, bx, by, bz, pre_ids)
            return adult if adult else None
        except Exception:
            return cand

    # ---------- events ----------
    @event_handler
    def on_actor_spawn(self, event: ActorSpawnEvent):
        a = event.actor
        if not isinstance(a, Mob):
            return
        s = self._s()

        if self._is_baby(a) and self._is_allowed(a.type, s):
            self._force_adult_replace(a)
            return

        rid = int(a.runtime_id)
        self._counts.setdefault(rid, 1)

        if self._is_leader(a):
            if self._counts.get(rid, 1) == 1:
                parsed = self._parse_count_from_name(a.name_tag)
                if parsed and parsed >= 1:
                    self._counts[rid] = min(parsed, int(s["max_stack_size"]))
            self._update_nametag(a)
        else:
            cnt = self._counts.get(rid, 1)
            if cnt >= 2 and self._eligible_basic(a, s, require_under_cap=True):
                self._promote_leader(a, min(cnt, int(s["max_stack_size"])))

        # keep stacks off tamed leaders on spawn
        self._defuse_tamed_leaders(s)

    # ===== lethal handling =====
    def _now_ticks(self) -> int:
        for attr in ("tick_count", "current_tick", "ticks"):
            v = getattr(self.server, attr, None)
            if isinstance(v, int):
                return v
        return int(time.monotonic() * 20.0)

    def _already_handled_this_tick(self, rid: int) -> bool:
        now = self._now_ticks()
        return self._death_handled_at.get(rid, -999999) == now

    def _note_handled_this_tick(self, rid: int) -> None:
        self._death_handled_at[rid] = self._now_ticks()

    def _is_lethal_hit(self, a: Mob, ev) -> bool:
        for name in ("will_die", "is_fatal", "is_lethal"):
            v = getattr(ev, name, None)
            if isinstance(v, bool):
                return v
        for name in ("new_health", "health_after", "resulting_health", "post_health"):
            v = getattr(ev, name, None)
            if isinstance(v, (int, float)):
                return v <= 0.0
        return False

    def _maybe_handle_lethal_hit(self, a: Mob, ev) -> None:
        if not isinstance(a, Mob) or not self._is_leader(a): return
        rid = int(getattr(a, "runtime_id", 0) or 0)
        if self._already_handled_this_tick(rid): return
        if not self._is_lethal_hit(a, ev): return
        if not self._s().get("handle_lethal_on_hurt", True):
            return
        self._process_leader_death(a)
        self._note_handled_this_tick(rid)

    if HAS_HURT:
        @event_handler
        def on_actor_hurt(self, event: 'ActorHurtEvent'):  # type: ignore[name-defined]
            try:
                self._maybe_handle_lethal_hit(getattr(event, "actor", None), event)
            except Exception:
                pass

    @event_handler
    def on_actor_death(self, event: ActorDeathEvent):
        a = event.actor
        try:
            self._breed_cooldown_until.pop(a.runtime_id, None)
            self._last_feed_pop.pop(a.runtime_id, None)
        except Exception:
            pass

        rid = int(getattr(a, "runtime_id", 0) or 0)
        if self._already_handled_this_tick(rid):
            if not isinstance(a, Mob) or not self._is_leader(a):
                self._counts.pop(rid, None)
            return

        if not isinstance(a, Mob):
            self._counts.pop(rid, None)
            return
        if not self._is_leader(a):
            self._counts.pop(rid, None)
            return

        self._process_leader_death(a)
        self._note_handled_this_tick(rid)

    # ---------- decrement + respawn (burn-proof minus-one) ----------
    def _process_leader_death(self, a: Mob) -> None:
        """Always respawn a replacement and carry the remaining count, regardless of death cause."""
        count = self._get_count(a)
        rid = int(getattr(a, "runtime_id", 0) or 0)

        if count <= 1:
            self._counts.pop(rid, None)
            return

        s = self._s()
        if not self._is_allowed(a.type, s):
            self._counts.pop(rid, None)
            return

        etype = self._normalize_id(a.type)
        loc = a.location
        dim_name = self._dim_token(a.dimension)
        remaining = count - 1

        # center on block to reduce summon fails on slabs/stairs
        bx, by, bz = self._block_center(loc.x, loc.y, loc.z)
        pre_ids = self._snapshot_same_block_ids(etype, dim_name, bx, by, bz)

        # Try immediate summon
        if not self._safe_summon(etype, dim_name, bx, by, bz):
            # Buffer the whole remaining stack and let the retry loop materialize a leader later
            key = (dim_name, int(bx // 1), int(by // 1), int(bz // 1), etype)
            self._pending[key] = self._pending.get(key, 0) + remaining
            self._counts.pop(rid, None)
            if not self._quiet():
                self.logger.warning(f"Summon failed; queued {remaining}x {etype} at {dim_name} {bx:.2f},{by:.2f},{bz:.2f}")
            return

        # Find newborn and attach remaining; broadened search to be resilient on environmental deaths.
        cap = int(s["max_stack_size"])

        newborn = (
            self._find_newborn_by_diff(etype, dim_name, bx, by, bz, pre_ids)
            or self._find_newborn_sameblock_or_near(etype, dim_name, bx, by, bz, pre_ids)
        )

        if newborn:
            newborn = self._adultize_and_return_sameblock(etype, dim_name, bx, by, bz, newborn)
            if newborn:
                self._promote_leader(newborn, min(remaining, cap))
                self._counts.pop(rid, None)
                return

        # If we got here, we summoned but couldn't resolve the newborn yet — schedule aggressive retries.
        self.server.scheduler.run_task(self, lambda: self._retry_attach_newborn(a, etype, dim_name, bx, by, bz, pre_ids, remaining, cap, s), delay=1)
        self.server.scheduler.run_task(self, lambda: self._retry_attach_newborn(a, etype, dim_name, bx, by, bz, pre_ids, remaining, cap, s), delay=3)
        self.server.scheduler.run_task(self, lambda: self._retry_attach_newborn(a, etype, dim_name, bx, by, bz, pre_ids, remaining, cap, s), delay=10)

    def _find_newborn_sameblock_or_near(self, etype: str, dim_name: str,
                                        bx: float, by: float, bz: float, pre_ids: set[int]):
        """More forgiving newborn resolver: same block preferred; also accept within ~0.9 blocks, y tol 1.5."""
        m = self._find_newborn_by_diff(etype, dim_name, bx, by, bz, pre_ids)
        if m:
            return m

        block_x, block_y, block_z = int(bx // 1), int(by // 1), int(bz // 1)
        best = None
        best_d2 = 9e18
        try:
            for cand in self._actors():
                if not isinstance(cand, Mob) or not cand.is_valid or cand.is_dead:
                    continue
                if self._normalize_id(cand.type) != etype:
                    continue
                if self._dim_token(cand.dimension) != dim_name:
                    continue
                if int(cand.runtime_id) in pre_ids:
                    continue
                lx, ly, lz = cand.location.x, cand.location.y, cand.location.z
                if abs(int(ly // 1) - block_y) > 1:
                    continue
                if abs(int(lx // 1) - block_x) > 1 or abs(int(lz // 1) - block_z) > 1:
                    continue
                dx, dz = (lx - bx), (lz - bz)
                d2 = dx * dx + dz * dz
                if d2 < best_d2 and d2 <= 0.9 * 0.9:
                    best_d2, best = d2, cand
        except Exception:
            return None
        return best

    # ---------- feed-to-pop ----------
    if HAS_INTERACT_ACTOR:
        @event_handler
        def on_player_interact_actor(self, event: PlayerInteractActorEvent):  # type: ignore[valid-type]
            self._handle_feed_pop_event(event, target_attr="actor")

    if HAS_INTERACT_ENTITY:
        @event_handler
        def on_player_interact_entity(self, event: PlayerInteractEntityEvent):  # type: ignore[valid-type]
            self._handle_feed_pop_event(event, target_attr="entity")

    def _handle_feed_pop_event(self, event, target_attr: str) -> None:
        s = self._s()
        if not s.get("feed_pop_enabled", True):
            return
        try:
            ent = getattr(event, target_attr, None) or getattr(event, "target", None)
            if not isinstance(ent, Mob): return
            if not self._is_leader(ent): return
            if self._is_baby(ent): return
            if self._is_tamed(ent) and s.get("ignore_tamed", True): return
            if not self._is_allowed(ent.type, s): return

            etype = self._normalize_id(ent.type)
            valid_items = BREED_ITEMS.get(etype, set())

            item_id = self._get_item_id_from_event_or_player(event)
            require_item = bool(s.get("feed_pop_require_item", True))
            item_ok = (item_id in valid_items) if item_id else False

            if require_item and not item_ok:
                return

            now = self._now_ticks()

            breed_cd = int(s.get("feed_pop_breed_cooldown_ticks", 6000))
            until = self._breed_cooldown_until.get(ent.runtime_id, 0)
            if now < until:
                return

            last = self._last_feed_pop.get(ent.runtime_id, 0)
            if (now - last) < int(s.get("feed_pop_cooldown_ticks", 6)):
                return
            self._last_feed_pop[ent.runtime_id] = now

            count = self._get_count(ent)
            if count < 2:
                return

            self._set_count(ent, count - 1)
            self._update_nametag(ent)

            dim_name = self._dim_token(ent.dimension)
            x, y, z = ent.location.x, ent.location.y, ent.location.z
            bx, by, bz = self._block_center(x, y, z)
            self._safe_summon(etype, dim_name, bx, by, bz)

            self._breed_cooldown_until[ent.runtime_id] = now + breed_cd

        except Exception as e:
            if not self._quiet():
                self.logger.warning(f"Feed-pop handler error: {e}")

    # ---------- periodic scan ----------
    def _schedule_scan(self) -> None:
        try:
            self.server.scheduler.cancel_tasks(self)
        except Exception:
            pass
        period = max(1, int(self._s()["scan_period_ticks"]))
        self.server.scheduler.run_task(self, self._scan_and_stack, delay=0, period=period)

    @staticmethod
    def _cell_key_of(m: Mob, cell: float) -> Tuple[object, int, int, int]:
        if cell <= 0.0:
            cell = 4.0
        loc = m.location
        return (m.dimension, int(loc.x // cell), int(loc.y // cell), int(loc.z // cell))

    @staticmethod
    def _cell_key_xyz(dim, x: float, y: float, z: float, cell: float) -> Tuple[object, int, int, int]:
        if cell <= 0.0:
            cell = 4.0
        return (dim, int(x // cell), int(y // cell), int(z // cell))

    def _neighbor_cells(self, base_key: Tuple[object, int, int, int]) -> List[Tuple[object, int, int, int]]:
        dim, cx, cy, cz = base_key
        out = []
        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                for dz in (-1, 0, 1):
                    out.append((dim, cx + dx, cy + dy, cz + dz))
        return out

    def _scan_and_stack(self) -> None:
        if self._maybe_reload_config():
            self._rebuild_allowed_cache()
            self._reindex_from_names()

        n = getattr(self, "_cd_prune_tick", 0) + 1
        if (n % 10) == 0:
            self._prune_cooldowns()
        self._cd_prune_tick = n

        # drain pending (resurrect stacks that couldn't attach earlier)
        if self._pending:
            cap = int(self._s()["max_stack_size"])
            drain = list(self._pending.items())[:8]
            for key, rem in drain:
                dim_name, bx, by, bz, etype = key
                if self._safe_summon(etype, dim_name, bx + 0.5, by + 0.5, bz + 0.5):
                    pre = set()
                    newborn = self._find_newborn_by_diff(etype, dim_name, bx + 0.5, by + 0.5, bz + 0.5, pre)
                    if not newborn:
                        newborn = self._find_newborn_sameblock_or_near(etype, dim_name, bx + 0.5, by + 0.5, bz + 0.5, pre)
                    if newborn:
                        self._promote_leader(newborn, min(rem, cap))
                        self._pending.pop(key, None)

        s = self._s()
        if not s.get("enabled", True) or not self._allowed_cache:
            return

        r = float(s["radius"])
        r2 = r * r
        min_group = int(s["min_group"])
        cap = int(s["max_stack_size"])

        # keep stacks off tamed leaders
        self._defuse_tamed_leaders(s)

        candidates = [a for a in self._actors()
                      if isinstance(a, Mob) and self._eligible_basic(a, s, allow_leader_sources=True)]
        if not candidates:
            return

        if s.get("allow_leader_pair_merge", False):
            self._pairwise_merge_fastpath_bucketed(candidates, s, max(r, 0.001), r2, cap)

        cell = r if r > 0 else 4.0
        buckets: dict[tuple[object, int, int, int], List[Mob]] = {}
        for a in candidates:
            key = self._cell_key_of(a, cell)
            buckets.setdefault(key, []).append(a)

        def neighbors(a: Mob):
            base = self._cell_key_of(a, cell)
            for nk in self._neighbor_cells(base):
                for m in buckets.get(nk, ()):
                    yield m

        visited: set[int] = set()

        for a in candidates:
            if a.runtime_id in visited:
                continue

            group: List[Mob] = [a]
            for b in neighbors(a):
                if b.runtime_id in visited or b.runtime_id == a.runtime_id:
                    continue
                if not self._same_type(a, b):
                    continue
                if self._within_radius_flat(a, b, r2) and self._eligible_basic(b, s):
                    group.append(b)

            if len(group) < min_group:
                for m in group:
                    visited.add(m.runtime_id)
                continue

            leader = self._choose_centroid_leader_under_cap(group, s)
            if leader is None:
                for m in group:
                    visited.add(m.runtime_id)
                continue

            leader_count = self._get_count(leader)
            space = max(0, cap - leader_count)
            if space <= 0:
                for m in group:
                    visited.add(m.runtime_id)
                continue

            absorbed = 0
            group_sorted = sorted(
                (m for m in group if m.runtime_id != leader.runtime_id),
                key=lambda m: ((m.location.x - leader.location.x) ** 2
                               + (m.location.z - leader.location.z) ** 2,
                               m.runtime_id)
            )

            for m in group_sorted:
                if absorbed >= space:
                    break
                if not self._eligible_basic(m, s, allow_leader_sources=True):
                    continue
                c = self._get_count(m)
                if c <= 0: c = 1
                if absorbed + c > space:
                    continue
                try:
                    self._counts.pop(int(m.runtime_id), None)
                    m.remove()
                except Exception:
                    continue
                visited.add(m.runtime_id)
                absorbed += c

            if absorbed > 0:
                visited.add(leader.runtime_id)
                self._promote_leader(leader, leader_count + absorbed)
            else:
                for m in group:
                    visited.add(m.runtime_id)

    # ===== leader fast-path bucketed =====
    def _pairwise_merge_fastpath_bucketed(self, mobs: List[Mob], s: dict[str, Any], cell: float, r2: float, cap: int) -> bool:
        changed = False
        checks = 0

        pool = [m for m in mobs
                if isinstance(m, Mob)
                and m.is_valid and not m.is_dead
                and self._is_leader(m)
                and self._eligible_basic(m, s, require_under_cap=True, allow_leader_sources=True)]

        if not pool:
            return False

        leaders_buckets: Dict[Tuple[object, int, int, int], List[Mob]] = defaultdict(list)
        for m in pool:
            leaders_buckets[self._cell_key_of(m, cell)].append(m)

        used: set[int] = set()

        for a in pool:
            if a.runtime_id in used:
                continue
            ca = self._get_count(a)
            if ca >= cap:
                continue

            base_key = self._cell_key_of(a, cell)
            best = None
            best_d2 = 9e18

            for nk in self._neighbor_cells(base_key):
                for b in leaders_buckets.get(nk, ()):
                    if checks >= MAX_FASTPAIR_CHECKS:
                        break
                    checks += 1

                    if b.runtime_id in used or b.runtime_id == a.runtime_id:
                        continue
                    if not self._same_type(a, b):
                        continue

                    cb = self._get_count(b)
                    if cb >= cap:
                        continue
                    if ca + cb > cap:
                        continue
                    if not self._within_radius_flat(a, b, r2):
                        continue

                    d2 = (a.location.x - b.location.x) ** 2 + (a.location.z - b.location.z) ** 2
                    if d2 < best_d2:
                        best_d2 = d2
                        best = b
                if checks >= MAX_FASTPAIR_CHECKS:
                    break

            if not best:
                continue

            leader = self._choose_centroid_leader_under_cap([a, best], s) or a
            source = best if leader.runtime_id == a.runtime_id else a
            total = ca + self._get_count(best)

            try:
                self._counts.pop(int(source.runtime_id), None)
                source.remove()
                self._promote_leader(leader, total)
                used.add(leader.runtime_id)
                used.add(source.runtime_id)
                changed = True
            except Exception:
                continue

            if checks >= MAX_FASTPAIR_CHECKS:
                break

        return changed

    # ---------- misc ----------
    def _actors(self):
        return self.server.level.actors

    def _silent_sender(self):
        if self._silent is not None:
            return self._silent
        base = self.server.command_sender
        if HAS_CMD_WRAPPER and CommandSenderWrapper is not None:
            try:
                self._silent = CommandSenderWrapper(
                    base,
                    on_message=lambda *_a, **_k: None,
                    on_error=lambda *_a, **_k: None,
                )
                return self._silent
            except Exception:
                pass

        class _SilentProxy:
            def __init__(self, inner): self._inner = inner
            def __getattr__(self, name): return getattr(self._inner, name)
            def send_message(self, *a, **k): return None
            def send_error(self, *a, **k): return None
            def send_raw_message(self, *a, **k): return None
            def has_permission(self, *_a, **_k): return True
        self._silent = _SilentProxy(base)
        return self._silent

    def _run_cmd(self, command: str) -> bool:
        try:
            self.server.dispatch_command(self._silent_sender(), command)
            return True
        except Exception:
            return False

    def _dim_token(self, dim_obj) -> str:
        try:
            name = getattr(dim_obj, "name", None) or getattr(dim_obj, "id", None) or ""
            name = str(name).lower()
            if "nether" in name or name.endswith("1"):
                return "the_nether"
            if "end" in name or name.endswith("2"):
                return "the_end"
        except Exception:
            pass
        return "overworld"

    def _safe_summon(self, etype: str, dim_name: str, x: float, y: float, z: float) -> bool:
        for dy in (0.51, 0.35, 0.20, 0.01, -0.20, -0.45, 0.70, -0.70, 1.00, -1.00):
            if self._run_cmd(f"execute in {dim_name} run summon {etype} {x:.2f} {y + dy:.2f} {z:.2f}"):
                return True
        for dx, dz in ((0.35, 0.0), (-0.35, 0.0), (0.0, 0.35), (0.0, -0.35)):
            if self._run_cmd(f"execute in {dim_name} run summon {etype} {x + dx:.2f} {y + 0.51:.2f} {z + dz:.2f}"):
                return True
        return False

    def _within_radius_flat(self, a: Mob, b: Mob, r2: float, ytol: float = 1.25) -> bool:
        pa, pb = a.location, b.location
        if abs(pa.y - pb.y) > ytol:
            return False
        dx, dz = (pa.x - pb.x), (pa.z - pb.z)
        return (dx * dx + dz * dz) <= r2

    def _eligible_basic(self, a: Mob, s: dict[str, Any],
                        require_under_cap: bool = False,
                        allow_leader_sources: bool = False) -> bool:
        if a.is_dead or not a.is_valid: return False
        if not self._is_allowed(a.type, s): return False
        if self._is_baby(a): return False
        if self._is_tamed(a) and s.get("ignore_tamed", True): return False
        nt = (getattr(a, "name_tag", "") or "").strip()
        if nt and not self._is_leader(a): return False
        if require_under_cap and self._at_cap(a, s): return False
        if self._is_leader(a) and not allow_leader_sources: return False
        return True

    def _choose_centroid_leader_under_cap(self, group: List[Mob], s: dict[str, Any]) -> Optional[Mob]:
        cx = sum(m.location.x for m in group) / len(group)
        cy = sum(m.location.y for m in group) / len(group)
        cz = sum(m.location.z for m in group) / len(group)

        undercap_leaders = [m for m in group if self._is_leader(m) and self._eligible_basic(m, s, require_under_cap=True)]
        if undercap_leaders:
            return min(undercap_leaders, key=lambda m: ((m.location.x - cx) ** 2 + (m.location.y - cy) ** 2 + (m.location.z - cz) ** 2, m.runtime_id))

        undercap_any = [m for m in group if self._eligible_basic(m, s, require_under_cap=True)]
        if undercap_any:
            return min(undercap_any, key=lambda m: ((m.location.x - cx) ** 2 + (m.location.y - cy) ** 2 + (m.location.z - cz) ** 2, m.runtime_id))
        return None

    def _at_cap(self, a: Mob, s: dict[str, Any]) -> bool:
        try:
            return self._get_count(a) >= int(s["max_stack_size"])
        except Exception:
            return False

    def _rebuild_allowed_cache(self) -> None:
        try:
            s = self._s()
            raw = s.get("allowed_types") or []
            self._allowed_cache = {self._normalize_id(x) for x in raw if isinstance(x, str) and x.strip()}
            if not self._allowed_cache and not self._quiet():
                self.logger.warning("MobStacker: allowed_types is empty → stacking disabled until fixed.")
        except Exception:
            self._allowed_cache = set()

    def _is_allowed(self, etype: str, s: dict[str, Any]) -> bool:
        et = self._normalize_id(etype)
        return et in self._allowed_cache

    def _is_leader(self, a) -> bool:
        return STACK_TAG in getattr(a, "scoreboard_tags", [])

    def _is_baby(self, a) -> bool:
        try:
            v = getattr(a, "is_baby", None)
            if isinstance(v, bool) and v: return True
            v = getattr(a, "baby", None)
            if isinstance(v, bool) and v: return True
            m = getattr(a, "isBaby", None)
            if callable(m) and m(): return True
            age = getattr(a, "age", None)
            if isinstance(age, (int, float)) and age < 0: return True
        except Exception:
            pass
        return False

    def _is_tamed(self, a) -> bool:
        try:
            v = getattr(a, "is_tamed", None)
            if isinstance(v, bool): return v
        except Exception:
            pass
        try:
            v = getattr(a, "tamed", None)
            if isinstance(v, bool): return v
        except Exception:
            pass
        try:
            m = getattr(a, "isTamed", None)
            if callable(m): return bool(m())
        except Exception:
            pass
        if getattr(a, "owner", None) or getattr(a, "owner_uuid", None):
            return True
        try:
            m = getattr(a, "hasOwner", None)
            if callable(m) and m(): return True
        except Exception:
            pass
        if getattr(a, "has_owner", None) is True:
            return True
        return False

    # -------- counts --------
    def _get_count(self, a: Mob) -> int:
        return int(self._counts.get(int(a.runtime_id), 1))

    def _set_count(self, a: Mob, value: int):
        v = max(1, int(value))
        self._counts[int(a.runtime_id)] = v

    def _promote_leader(self, a: Mob, count: int):
        cap = int(self._s()["max_stack_size"])
        if self._is_baby(a):
            etype = self._normalize_id(a.type)
            dim_name = self._dim_token(a.dimension)
            bx, by, bz = self._block_center(a.location.x, a.location.y, a.location.z)
            pre_ids = self._snapshot_same_block_ids(etype, dim_name, bx, by, bz)
            self._force_adult_replace(a)
            adult = self._find_newborn_by_diff(etype, dim_name, bx, by, bz, pre_ids)
            if adult:
                a = adult
            else:
                return
        try:
            a.add_scoreboard_tag(STACK_TAG)
        except Exception:
            pass
        self._set_count(a, max(1, min(int(count), cap)))
        self._update_nametag(a)

    def _update_nametag(self, a: Mob):
        count = self._get_count(a)
        threshold = int(self._s().get("show_name_for_count_ge", 2))
        if count < threshold:
            try:
                a.name_tag = ""
                a.is_name_tag_visible = False
            except Exception:
                pass
            return
        label = self._s().get("label_format", NAME_FORMAT)
        if "{count}" not in label:
            label = NAME_FORMAT
        a.name_tag = label.format(count=count) + SIGNATURE
        a.is_name_tag_always_visible = True
        a.is_name_tag_visible = True

    # ===== defuse tamed leaders =====
    def _defuse_tamed_leaders(self, s: dict[str, Any]) -> None:
        if not s.get("ignore_tamed", True):
            return

        cap = int(s["max_stack_size"])
        r = float(s["radius"])
        cell = r if r > 0 else 4.0

        try:
            actors = [a for a in self._actors() if isinstance(a, Mob) and a.is_valid and not a.is_dead]
            if not any(self._is_leader(a) and self._is_tamed(a) for a in actors):
                return

            index: Dict[str, Dict[Tuple[object, int, int, int], List[Mob]]] = defaultdict(lambda: defaultdict(list))
            for m in actors:
                if self._is_tamed(m):
                    continue
                if not self._eligible_basic(m, s, require_under_cap=True):
                    continue
                t = self._normalize_id(m.type)
                index[t][self._cell_key_of(m, cell)].append(m)

            for a in actors:
                if not self._is_leader(a):
                    continue
                if not self._is_tamed(a):
                    continue

                t = self._normalize_id(a.type)
                base_key = self._cell_key_of(a, cell)

                best = None
                bestd2 = 9e18
                for nk in self._neighbor_cells(base_key):
                    for m in index[t].get(nk, ()):
                        if not m.is_valid or m.is_dead:
                            continue
                        d2 = (m.location.x - a.location.x) ** 2 + (m.location.z - a.location.z) ** 2
                        if d2 < bestd2:
                            bestd2, best = d2, m

                if best:
                    self._promote_leader(best, min(self._get_count(a), cap))

                rid = int(a.runtime_id)
                self._counts.pop(rid, None)
                self._breed_cooldown_until.pop(a.runtime_id, None)
                self._last_feed_pop.pop(a.runtime_id, None)
                try:
                    a.remove_scoreboard_tag(STACK_TAG)
                except Exception:
                    pass
                try:
                    a.name_tag = ""
                    a.is_name_tag_visible = False
                except Exception:
                    pass

        except Exception as e:
            if not self._quiet():
                self.logger.warning(f"Defuse tamed leaders failed: {e!r}")

    # ---------- newborn helpers ----------
    @staticmethod
    def _block_center(x: float, y: float, z: float) -> tuple[float, float, float]:
        import math
        bx = math.floor(x) + 0.5
        by = math.floor(y) + 0.5
        bz = math.floor(z) + 0.5
        return bx, by, bz

    def _snapshot_same_block_ids(self, etype: str, dim_name: str, bx: float, by: float, bz: float) -> set[int]:
        block_x, block_y, block_z = int(bx // 1), int(by // 1), int(bz // 1)
        ids: set[int] = set()
        try:
            for m in self._actors():
                if not isinstance(m, Mob) or not m.is_valid or m.is_dead:
                    continue
                if self._normalize_id(m.type) != etype:
                    continue
                if self._dim_token(m.dimension) != dim_name:
                    continue
                lx, ly, lz = m.location.x, m.location.y, m.location.z
                if int(lx // 1) == block_x and int(ly // 1) == block_y and int(lz // 1) == block_z:
                    ids.add(int(m.runtime_id))
        except Exception:
            pass
        return ids

    def _find_newborn_by_diff(self, etype: str, dim_name: str, bx: float, by: float, bz: float, pre_ids: set[int]):
        block_x, block_y, block_z = int(bx // 1), int(by // 1), int(bz // 1)
        best = None
        best_d2 = 9e18
        try:
            for m in self._actors():
                if not isinstance(m, Mob) or not m.is_valid or m.is_dead:
                    continue
                if self._normalize_id(m.type) != etype:
                    continue
                if self._dim_token(m.dimension) != dim_name:
                    continue
                if int(m.runtime_id) in pre_ids:
                    continue
                lx, ly, lz = m.location.x, m.location.y, m.location.z
                if int(lx // 1) != block_x or int(ly // 1) != block_y or int(lz // 1) != block_z:
                    continue
                d2 = (lx - bx) * (lx - bx) + (lz - bz) * (lz - bz)
                if d2 < best_d2:
                    best_d2, best = d2, m
        except Exception:
            return None
        return best

    def _retry_attach_newborn(self, old_leader, etype, dim_name, bx, by, bz, pre_ids, remaining, cap, s):
        newborn = self._find_newborn_by_diff(etype, dim_name, bx, by, bz, pre_ids)
        if not newborn:
            newborn = self._find_newborn_sameblock_or_near(etype, dim_name, bx, by, bz, pre_ids)
        if newborn:
            newborn = self._adultize_and_return_sameblock(etype, dim_name, bx, by, bz, newborn)
            if newborn:
                self._promote_leader(newborn, min(remaining, cap))
                self._counts.pop(int(getattr(old_leader, "runtime_id", 0) or 0), None)
                return

        # Fallback within radius (flat), under cap, y tol 1.25
        r = float(s["radius"])
        r2 = r * r
        best = None
        best_flat_d2 = 9e18
        for m in self._actors():
            if not isinstance(m, Mob) or not m.is_valid or m.is_dead:
                continue
            if self._normalize_id(m.type) != etype:
                continue
            if self._dim_token(m.dimension) != dim_name:
                continue
            if not self._eligible_basic(m, s, require_under_cap=True):
                continue
            if abs(m.location.y - by) > 1.25:
                continue
            dx = m.location.x - bx
            dz = m.location.z - bz
            flat_d2 = dx * dx + dz * dz
            if flat_d2 <= r2 and flat_d2 < best_flat_d2:
                best_flat_d2 = flat_d2
                best = m
        if best:
            best = self._adultize_and_return_sameblock(etype, dim_name, bx, by, bz, best)
            if best:
                self._promote_leader(best, min(remaining, cap))
                self._counts.pop(int(getattr(old_leader, "runtime_id", 0) or 0), None)
                return

        # As a final fallback, queue pending to ensure we don't drop the stack.
        key = (dim_name, int(bx // 1), int(by // 1), int(bz // 1), etype)
        self._pending[key] = self._pending.get(key, 0) + remaining

    # ---------- pruning ----------
    def _prune_cooldowns(self) -> None:
        try:
            live = {
                int(a.runtime_id)
                for a in self._actors()
                if getattr(a, "is_valid", False) and not getattr(a, "is_dead", False)
            }
            for rid in list(self._breed_cooldown_until.keys()):
                if rid not in live:
                    self._breed_cooldown_until.pop(rid, None)
            for rid in list(self._last_feed_pop.keys()):
                if rid not in live:
                    self._last_feed_pop.pop(rid, None)
            for rid in list(self._death_handled_at.keys()):
                if rid not in live:
                    self._death_handled_at.pop(rid, None)
            for rid in list(self._counts.keys()):
                if rid not in live:
                    self._counts.pop(rid, None)
        except Exception:
            pass

    # ---------- config ----------
    def _defaults(self) -> dict:
        return {
            "stacking": {
                "enabled": True,
                "radius": 3.0,
                "min_group": 5,
                "max_stack_size": 100,
                "scan_period_ticks": 60,
                "label_format": NAME_FORMAT,
                "ignore_tamed": True,
                "feed_pop_enabled": True,
                "feed_pop_require_item": True,
                "feed_pop_cooldown_ticks": 6,
                "feed_pop_breed_cooldown_ticks": 6000,
                "debug_feed_pop": False,
                "handle_lethal_on_hurt": True,
                "show_name_for_count_ge": 2,
                "quiet_console": True,
                "silence_command_feedback": True,
                "allow_leader_pair_merge": False,
            }
        }

    def _cfg_path(self) -> Path:
        Path(self.data_folder).mkdir(parents=True, exist_ok=True)
        return Path(self.data_folder) / "config.toml"

    def _write_example_config(self) -> None:
        path = self._cfg_path()
        if path.exists():
            return

        example = r"""# ===================== MobStacker — config.toml =====================
# Distances are in blocks. Time is in ticks (20 ticks ≈ 1 second).

[stacking]
enabled = true
radius = 3.0
min_group = 5
max_stack_size = 100
scan_period_ticks = 60
label_format = "×{count}"
show_name_for_count_ge = 2
ignore_tamed = true

# Feed-to-pop
feed_pop_enabled = true
feed_pop_require_item = true
feed_pop_cooldown_ticks = 6
feed_pop_breed_cooldown_ticks = 6000
debug_feed_pop = false

# Lethal-on-hurt (strict). If false, only ActorDeathEvent decrements.
handle_lethal_on_hurt = true

# Pair-merge of leaders within radius before min_group (off by default)
allow_leader_pair_merge = false

quiet_console = true
silence_command_feedback = true

# Only mobs in this list will stack.
allowed_types = [
  # "minecraft:cow",
  # "minecraft:sheep",
  # "minecraft:chicken",
  # "minecraft:pig",
]
"""
        path.write_text(example, encoding="utf-8")
        self._cfg = {"stacking": {
            **self._defaults()["stacking"],
            "allowed_types": [],
        }}
        try:
            self._cfg_mtime = path.stat().st_mtime
        except Exception:
            self._cfg_mtime = None

    def _ensure_comment_preserving_defaults(self) -> None:
        self._cfg = {"stacking": {
            **self._defaults()["stacking"],
        }}

    def _s(self) -> dict[str, Any]:
        base = self._defaults()
        root = {**base, **(self._cfg or {})}
        root["stacking"] = {**base["stacking"], **(self._cfg.get("stacking", {}) if self._cfg else {})}
        return root["stacking"]

    def _quiet(self) -> bool:
        try:
            return bool(self._s().get("quiet_console", True))
        except Exception:
            return True

    def _maybe_reload_config(self) -> bool:
        path = self._cfg_path()
        try:
            m = path.stat().st_mtime
        except Exception:
            m = None
        if m is not None and m != self._cfg_mtime:
            self._load_or_create_config()
            if not self._quiet():
                s = self._s()
                self.logger.info(
                    f"MobStacker config reloaded (radius={s['radius']}, min_group={s['min_group']}, "
                    f"max_stack={s['max_stack_size']}; {len(s['allowed_types'])} allowed types)"
                )
            return True
        return False

    def _load_or_create_config(self) -> None:
        path = self._cfg_path()
        if not path.exists():
            self._write_example_config()
            return

        tomllib = None
        try:
            import tomllib as _tomllib  # py311+
            tomllib = _tomllib
        except Exception:
            tomllib = None

        try:
            if tomllib:
                with path.open("rb") as f:
                    self._cfg = tomllib.load(f)
            else:
                self._cfg = self._parse_toml_simple(path.read_text(encoding="utf-8"))
        except Exception:
            if not self._quiet():
                self.logger.warning("MobStacker: config parse failed; using built-in defaults for this run.")
            self._ensure_comment_preserving_defaults()

        if "stacking" not in (self._cfg or {}):
            self._cfg = {"stacking": {}}

        try:
            self._cfg_mtime = path.stat().st_mtime
        except Exception:
            self._cfg_mtime = None

    def _save_config(self) -> None:
        path = self._cfg_path()
        path.parent.mkdir(parents=True, exist_ok=True)

        data = self._defaults()

        def merge(a: dict, b: dict):
            for k, v in b.items():
                if isinstance(v, dict) and isinstance(a.get(k), dict):
                    merge(a[k], v)
                else:
                    a[k] = v

        merge(data, self._cfg or {})

        try:
            import tomli_w as _tomli_w  # type: ignore
        except Exception:
            _tomli_w = None

        if _tomli_w:
            path.write_text(_tomli_w.dumps(data), encoding="utf-8")
        else:
            s = data["stacking"]
            allow = ", ".join(f'"{t}"' for t in (s.get("allowed_types") or []))
            lines = [
                "[stacking]",
                f"enabled = {str(bool(s.get('enabled', True))).lower()}",
                f"radius = {float(s['radius'])}",
                f"min_group = {int(s['min_group'])}",
                f"max_stack_size = {int(s['max_stack_size'])}",
                f"scan_period_ticks = {int(s['scan_period_ticks'])}",
                f'label_format = "{s.get("label_format", NAME_FORMAT)}"',
                f"ignore_tamed = {str(bool(s.get('ignore_tamed', True))).lower()}",
                f"feed_pop_enabled = {str(bool(s.get('feed_pop_enabled', True))).lower()}",
                f"feed_pop_require_item = {str(bool(s.get('feed_pop_require_item', True))).lower()}",
                f"feed_pop_cooldown_ticks = {int(s.get('feed_pop_cooldown_ticks', 6))}",
                f"feed_pop_breed_cooldown_ticks = {int(s.get('feed_pop_breed_cooldown_ticks', 6000))}",
                f"debug_feed_pop = {str(bool(s.get('debug_feed_pop', False))).lower()}",
                f"handle_lethal_on_hurt = {str(bool(s.get('handle_lethal_on_hurt', True))).lower()}",
                f"show_name_for_count_ge = {int(s.get('show_name_for_count_ge', 2))}",
                f"quiet_console = {str(bool(s.get('quiet_console', True))).lower()}",
                f"silence_command_feedback = {str(bool(s.get('silence_command_feedback', True))).lower()}",
                f"allow_leader_pair_merge = {str(bool(s.get('allow_leader_pair_merge', False))).lower()}",
                f"allowed_types = [{allow}]",
                "",
            ]
            path.write_text("\n".join(lines), encoding="utf-8")

    # ---------- tiny TOML parser ----------
    def _parse_toml_simple(self, text: str) -> dict:
        text = text.lstrip("\ufeff")
        out = {"stacking": {}}
        section = None
        for raw in text.splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("[") and line.endswith("]"):
                section = line[1:-1].strip()
                continue
            if "=" not in line:
                continue
            key, val = [p.strip() for p in line.split("=", 1)]
            if section != "stacking":
                continue
            if val.startswith("[") and val.endswith("]"):
                inner = val[1:-1].strip()
                items = []
                if inner:
                    for part in inner.split(","):
                        part = part.strip()
                        if part.startswith('"') and part.endswith('"'):
                            items.append(part[1:-1])
                        elif part.startswith("'") and part.endswith("'"):
                            items.append(part[1:-1])
                out["stacking"][key] = items
                continue
            if (val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'")):
                out["stacking"][key] = val[1:-1]
                continue
            low = val.lower()
            if low in ("true", "false"):
                out["stacking"][key] = (low == "true")
                continue
            try:
                if "." in val or "e" in low:
                    out["stacking"][key] = float(val)
                else:
                    out["stacking"][key] = int(val)
            except Exception:
                out["stacking"][key] = val
        return out

    # ---------- memory rebuild ----------
    def _reindex_from_names(self) -> None:
        self._counts.clear()
        cap = int(self._s()["max_stack_size"])
        for a in self._actors():
            if not isinstance(a, Mob) or not a.is_valid or a.is_dead:
                continue
            rid = int(a.runtime_id)
            if self._is_leader(a):
                parsed = self._parse_count_from_name(a.name_tag)
                if parsed and parsed >= 1:
                    self._counts[rid] = min(parsed, cap)
                else:
                    self._counts[rid] = 1
                    try:
                        a.name_tag = ""
                        a.is_name_tag_visible = False
                    except Exception:
                        pass
            else:
                self._counts[rid] = 1

    @staticmethod
    def _parse_count_from_name(name: Optional[str]) -> Optional[int]:
        if not name:
            return None
        s = str(name)
        if not s.endswith(SIGNATURE):
            return None
        base = s[:-len(SIGNATURE)].strip()
        m = re.search(r'(?:×|x)\s*(\d+)\s*$', base)
        try:
            return int(m.group(1)) if m else None
        except Exception:
            return None

    # ---------- helpers for feed-pop (item id) ----------
    def _get_item_id_from_event_or_player(self, event) -> Optional[str]:
        try:
            pl = getattr(event, "player", None) or getattr(event, "source", None)
            if not pl:
                return None
            inv = getattr(pl, "inventory", None)
            if inv and hasattr(inv, "item_in_main_hand"):
                it = inv.item_in_main_hand
                if callable(it): it = it()
                if it and getattr(it, "type", None):
                    return str(it.type)
            it = getattr(pl, "held_item", None)
            if callable(it): it = it()
            if it and getattr(it, "type", None):
                return str(it.type)
        except Exception:
            pass
        return None
