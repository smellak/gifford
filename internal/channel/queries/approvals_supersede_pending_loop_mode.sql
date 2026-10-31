UPDATE approval_requests
SET state = 'SUPERSEDED', resolution_json = NULL,
  resolved_by = ?, resolved_at = ?
WHERE loop_id = ? AND state = 'PENDING'
  AND (
    source = 'loop_pattern'
    OR (source = 'loop_budget' AND ? = 'STEP')
    OR (
      source = 'loop_step' AND ? = 'AUTO'
      AND EXISTS (
        SELECT 1
        FROM channel_control_decisions AS decision
        WHERE decision.object_type = approval_requests.object_type
          AND decision.object_id = approval_requests.object_id
          AND decision.stage IN ('admission', 'continuation')
          AND decision.outcome = 'APPROVAL'
          AND decision.reason = 'LOOP_STEP_TOKEN_REQUIRED'
      )
    )
  )
