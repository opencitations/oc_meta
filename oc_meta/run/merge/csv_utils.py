# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from __future__ import annotations


def parse_merged_entities(merged_entities: str) -> list[str]:
    return [entity.strip() for entity in merged_entities.split(";") if entity.strip()]
