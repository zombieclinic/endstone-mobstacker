# src/endstone_mobstacker/menuui.py
from typing import Any, List
import json

from endstone import Player
from endstone.form import ActionForm, ModalForm, TextInput  # keep imports minimal/compatible

def _split_modal_payload(data: Any, expect: int) -> List[Any]:
    if isinstance(data, list):
        vals = list(data)
    else:
        try:
            vals = json.loads(data) if isinstance(data, str) else []
            if not isinstance(vals, list):
                vals = []
        except Exception:
            vals = []
    if len(vals) < expect:
        vals += [None] * (expect - len(vals))
    else:
        vals = vals[:expect]
    return vals

def _zero_one(v: Any, default: int) -> int:
    try:
        n = int(float(v))
        return 1 if n >= 1 else 0
    except Exception:
        return 1 if default else 0

def _as_bool01(v: int) -> bool:
    return bool(int(v) >= 1)

def _clean_id(s: str) -> str:
    t = (s or "").strip().lower()
    if not t: return ""
    return t if ":" in t else "minecraft:" + t

def _present(plugin, pl: Player, form) -> None:
    for name in ("open", "send_to", "send", "present", "display"):
        m = getattr(form, name, None)
        if callable(m): m(pl); return
    for name in ("open_form", "send_form", "show_form", "present_form", "display_form"):
        m = getattr(pl, name, None)
        if callable(m): m(form); return
    try:
        svc = getattr(plugin.server, "forms", None)
        if svc:
            for name in ("show", "present", "send"):
                m = getattr(svc, name, None)
                if callable(m): m(pl, form); return
    except Exception:
        pass
    raise RuntimeError("No compatible method to present forms on this Endstone build.")

class MobstackerMenu:
    def __init__(self, plugin):
        self.p = plugin  # MobStacker instance

    def open_main(self, pl: Player) -> None:
        s = self.p._s()
        content_lines = [
            "§7Configure MobStacker. Values save to disk.",
            "§8(Where noted: 0 = off, 1 = on)",
            "",
            "§lNow:",
            f"§f• enabled: {1 if s.get('enabled', True) else 0}",
            f"§f• radius: {s.get('radius', 3.0)}",
            f"§f• min_group: {s.get('min_group', 5)}",
            f"§f• max_stack_size: {s.get('max_stack_size', 100)}",
            f"§f• show_name_for_count_ge: {s.get('show_name_for_count_ge', 2)}",
        ]
        f = ActionForm(title="§f§lMobStacker — Admin", content="\n".join(content_lines))
        f.add_button("⚙️  Edit basic values", on_click=lambda p: self._edit_basics_values(p))
        f.add_button("📜  Allowed types",     on_click=lambda p: self._edit_allowed_types(p))
        f.add_button("🔄  Reload config",     on_click=lambda p: self._reload_cfg(p))
        f.add_button("🧮  Force rescan",      on_click=lambda p: self._force_rescan(p))
        f.add_button("Close")
        f.on_close = lambda p: None
        _present(self.p, pl, f)

    def _reload_cfg(self, pl: Player):
        try:
            self.p._load_or_create_config()
            self.p._rebuild_allowed_cache()
            self.p._schedule_scan()
            pl.send_message("§aConfig reloaded from disk.")
        except Exception as e:
            try: pl.send_message(f"§cReload failed: {e}")
            except Exception: pass
        self.open_main(pl)

    def _force_rescan(self, pl: Player):
        try:
            self.p._scan_and_stack()
            pl.send_message("§aScan executed.")
        except Exception as e:
            try: pl.send_message(f"§cScan failed: {e}")
            except Exception: pass
        self.open_main(pl)

    # ===== Basics (values) =====
    def _edit_basics_values(self, pl: Player) -> None:
        s = self.p._s()
        form = ModalForm(title="§f§lMobStacker — Basics (Values)")

        # Flags (0/1)
        form.add_control(TextInput("enabled (0/1)", "1", "1" if s.get("enabled", True) else "0"))

        # Numbers
        form.add_control(TextInput("radius (blocks)", "3.0", str(s.get("radius", 3.0))))
        form.add_control(TextInput("min_group to form a stack", "5", str(s.get("min_group", 5))))
        form.add_control(TextInput("max_stack_size cap", "100", str(s.get("max_stack_size", 100))))
        form.add_control(TextInput("show_name_for_count_ge", "2", str(s.get("show_name_for_count_ge", 2))))

        # Label
        form.add_control(TextInput("label_format (use {count})", "×{count}", str(s.get("label_format", "×{count}"))))
        form.submit_button = "Save"

        def on_submit(player: Player, data: str):
            vals = _split_modal_payload(data, 6)

            enabled01 = _zero_one(vals[0], 1 if s.get("enabled", True) else 0)

            try:    radius = float(vals[1])
            except: radius = float(s.get("radius", 3.0))
            try:    ming = int(float(vals[2]))
            except: ming = int(s.get("min_group", 5))
            try:    cap = int(float(vals[3]))
            except: cap = int(s.get("max_stack_size", 100))
            try:    showge = int(float(vals[4]))
            except: showge = int(s.get("show_name_for_count_ge", 2))

            label = str(vals[5]) if vals[5] else s.get("label_format", "×{count}")
            if "{count}" not in label:
                label = "×{count}"

            self.p._cfg.setdefault("stacking", {})
            st = self.p._cfg["stacking"]
            st["enabled"] = _as_bool01(enabled01)
            st["radius"] = radius
            st["min_group"] = ming
            st["max_stack_size"] = cap
            st["show_name_for_count_ge"] = showge
            st["label_format"] = label

            self.p._save_config()
            try:
                save_json = getattr(self.p, "_save_config_json", None)
                if callable(save_json):
                    save_json()
            except Exception:
                pass

            self.p._rebuild_allowed_cache()
            self.p._schedule_scan()
            try: player.send_message("§aBasics saved to disk.")
            except Exception: pass
            self.open_main(player)

        form.on_submit = on_submit
        form.on_close = lambda p: self.open_main(p)
        _present(self.p, pl, form)

    # ===== Allowed Types =====
    def _edit_allowed_types(self, pl: Player) -> None:
        s = self.p._s()
        allow = list(s.get("allowed_types") or [])

        lines = ["§7Only types in this list will stack.", ""]
        if not allow:
            lines.append("§8(Empty — add at least one to enable stacking.)")
        else:
            lines.append("§lCurrent:")
            lines += [f"§f• {t}" for t in allow]

        f = ActionForm(title="§f§lMobStacker — Allowed Types", content="\n".join(lines))
        for t in allow:
            f.add_button(f"Remove {t}", on_click=lambda p, t=t: self._confirm_remove(p, t))
        f.add_button("Add a type…", on_click=lambda p: self._add_type(p))
        f.add_button("Back")
        f.on_close = lambda p: self.open_main(p)
        _present(self.p, pl, f)

    def _confirm_remove(self, pl: Player, etype: str) -> None:
        s = self.p._s()
        allow = list(s.get("allowed_types") or [])
        if etype not in allow:
            try: pl.send_message("§7Already removed.")
            except Exception: pass
            return self._edit_allowed_types(pl)

        allow = [t for t in allow if t != etype]
        self._write_allow(allow)
        try: pl.send_message(f"§aRemoved {etype}.")
        except Exception: pass
        self._edit_allowed_types(pl)

    def _add_type(self, pl: Player) -> None:
        frm = ModalForm(title="§f§lAdd Allowed Type")
        frm.add_control(TextInput("Entity type ID (e.g., minecraft:cow)", "minecraft:cow", "minecraft:cow"))
        frm.submit_button = "Add"

        def on_submit(player: Player, data: str):
            vals = _split_modal_payload(data, 1)
            t = _clean_id(str(vals[0] or ""))
            if not t:
                try: player.send_message("§cInvalid type.")
                except Exception: pass
                return self._edit_allowed_types(player)

            s = self.p._s()
            allow = list(s.get("allowed_types") or [])
            if t in allow:
                try: player.send_message("§7Already present.")
                except Exception: pass
                return self._edit_allowed_types(player)

            allow.append(t)
            self._write_allow(allow)
            try: player.send_message(f"§aAdded {t}.")
            except Exception: pass
            self._edit_allowed_types(player)

        frm.on_submit = on_submit
        frm.on_close = lambda p: self._edit_allowed_types(p)
        _present(self.p, pl, frm)

    def _write_allow(self, allow: List[str]) -> None:
        self.p._cfg.setdefault("stacking", {})
        self.p._cfg["stacking"]["allowed_types"] = allow
        self.p._save_config()
        try:
            save_json = getattr(self.p, "_save_config_json", None)
            if callable(save_json): save_json()
        except Exception:
            pass
        self.p._rebuild_allowed_cache()
