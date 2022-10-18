package keptnappversion

import (
	"context"
	"fmt"

	klcv1alpha1 "github.com/keptn-sandbox/lifecycle-controller/operator/api/v1alpha1"
	"github.com/keptn-sandbox/lifecycle-controller/operator/api/v1alpha1/common"
	"github.com/keptn-sandbox/lifecycle-controller/operator/api/v1alpha1/semconv"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func (r *KeptnAppVersionReconciler) reconcilePrePostEvaluation(ctx context.Context, appVersion *klcv1alpha1.KeptnAppVersion, checkType common.CheckType) (common.KeptnState, error) {
	newStatus, state, err := r.reconcileEvaluations(ctx, checkType, appVersion)
	if err != nil {
		return common.StateUnknown, err
	}
	overallState := common.GetOverallState(state)

	switch checkType {
	case common.PreEvaluationCheckType:
		appVersion.Status.PreEvaluationStatus = overallState
		appVersion.Status.PreEvaluationTaskStatus = newStatus
	case common.PostEvaluationCheckType:
		appVersion.Status.PostEvaluationStatus = overallState
		appVersion.Status.PostEvaluationTaskStatus = newStatus
	}

	// Write Status Field
	err = r.Client.Status().Update(ctx, appVersion)
	if err != nil {
		return common.StateUnknown, err
	}
	return overallState, nil
}

func (r *KeptnAppVersionReconciler) reconcileEvaluations(ctx context.Context, checkType common.CheckType, appVersion *klcv1alpha1.KeptnAppVersion) ([]klcv1alpha1.KeptnEvaluationStatus, common.StatusSummary, error) {
	phase := common.KeptnPhaseType{
		ShortName: "ReconcileEvaluations",
		LongName:  "Reconcile Evaluations",
	}

	var evaluations []string
	var statuses []klcv1alpha1.EvaluationStatus

	switch checkType {
	case common.PreEvaluationCheckType:
		evaluations = appVersion.Spec.PreDeploymentEvaluations
		statuses = appVersion.Status.PreEvaluationTaskStatus
	case common.PostEvaluationCheckType:
		evaluations = appVersion.Spec.PostDeploymentEvaluations
		statuses = appVersion.Status.PostEvaluationTaskStatus
	}

	var summary common.StatusSummary
	summary.Total = len(evaluations)
	// Check current state of the PrePostEvaluationTasks
	var newStatus []klcv1alpha1.EvaluationStatus
	for _, evaluation := range evaluations {
		var oldstatus common.KeptnState
		for _, ts := range statuses {
			if ts.TaskDefinitionName == taskDefinitionName {
				oldstatus = ts.Status
			}
		}

		taskStatus := GetTaskStatus(taskDefinitionName, statuses)
		task := &klcv1alpha1.KeptnTask{}
		taskExists := false

		if oldstatus != taskStatus.Status {
			r.recordEvent(phase, "Normal", appVersion, "TaskStatusChanged", fmt.Sprintf("task status changed from %s to %s", oldstatus, taskStatus.Status))
		}

		// Check if task has already succeeded or failed
		if taskStatus.Status == common.StateSucceeded || taskStatus.Status == common.StateFailed {
			newStatus = append(newStatus, taskStatus)
			continue
		}

		// Check if Task is already created
		if taskStatus.TaskName != "" {
			err := r.Client.Get(ctx, types.NamespacedName{Name: taskStatus.TaskName, Namespace: appVersion.Namespace}, task)
			if err != nil && errors.IsNotFound(err) {
				taskStatus.TaskName = ""
			} else if err != nil {
				return nil, summary, err
			}
			taskExists = true
		}

		// Create new Task if it does not exist
		if !taskExists {
			taskName, err := r.createKeptnTask(ctx, appVersion.Namespace, appVersion, taskDefinitionName, checkType)
			if err != nil {
				return nil, summary, err
			}
			taskStatus.TaskName = taskName
			taskStatus.SetStartTime()
		} else {
			// Update state of Task if it is already created
			taskStatus.Status = task.Status.Status
			if taskStatus.Status.IsCompleted() {
				taskStatus.SetEndTime()
			}
		}
		// Update state of the Check
		newStatus = append(newStatus, taskStatus)
	}

	for _, ns := range newStatus {
		summary = common.UpdateStatusSummary(ns.Status, summary)
	}
	if common.GetOverallState(summary) != common.StateSucceeded {
		r.recordEvent(phase, "Warning", appVersion, "NotFinished", "has not finished")
	}
	return newStatus, summary, nil
}

func (r *KeptnAppVersionReconciler) createKeptnEvaluation(ctx context.Context, namespace string, appVersion *klcv1alpha1.KeptnAppVersion, taskDefinition string, checkType common.CheckType) (string, error) {

	ctx, span := r.Tracer.Start(ctx, "create_app_task", trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()

	semconv.AddAttributeFromAppVersion(span, *appVersion)

	// create TraceContext
	// follow up with a Keptn propagator that JSON-encoded the OTel map into our own key
	traceContextCarrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, traceContextCarrier)

	phase := common.KeptnPhaseType{
		ShortName: "KeptnTaskCreate",
		LongName:  "Keptn Task Create",
	}

	newTask := &klcv1alpha1.KeptnTask{
		ObjectMeta: metav1.ObjectMeta{
			Name:        common.GenerateTaskName(checkType, taskDefinition),
			Namespace:   namespace,
			Annotations: traceContextCarrier,
		},
		Spec: klcv1alpha1.KeptnTaskSpec{
			Version:          appVersion.Spec.Version,
			AppName:          appVersion.Spec.AppName,
			TaskDefinition:   taskDefinition,
			Parameters:       klcv1alpha1.TaskParameters{},
			SecureParameters: klcv1alpha1.SecureParameters{},
			Type:             checkType,
		},
	}
	err := controllerutil.SetControllerReference(appVersion, newTask, r.Scheme)
	if err != nil {
		r.Log.Error(err, "could not set controller reference:")
	}
	err = r.Client.Create(ctx, newTask)
	if err != nil {
		r.Log.Error(err, "could not create KeptnTask")
		r.recordEvent(phase, "Warning", appVersion, "CreateFailed", "could not create KeptnTask")
		return "", err
	}
	r.recordEvent(phase, "Normal", appVersion, "Created", "created")

	return newTask.Name, nil
}

func GetEvaluationStatus(taskName string, instanceStatus []klcv1alpha1.TaskStatus) klcv1alpha1.TaskStatus {
	for _, status := range instanceStatus {
		if status.TaskDefinitionName == taskName {
			return status
		}
	}
	return klcv1alpha1.TaskStatus{
		TaskDefinitionName: taskName,
		Status:             common.StatePending,
		TaskName:           "",
	}
}
