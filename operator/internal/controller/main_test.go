package controller

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/yaml"

	kvv1 "github.com/Mohamed-Magdy-Dewidar/distributed-kv-data-store/operator/api/v1alpha1"
)

// These tests run the reconciler against a real kube-apiserver (envtest).
// It authenticates as a service account bound to config/rbac/role.yaml, the
// role generated from the RBAC markers, so a request the role does not allow
// fails here as it would in a cluster. The test itself uses an admin client
// to play the parts envtest lacks: the StatefulSet controller (pods, their
// status), the kubelet (pod readiness) and the PVC controller.

var (
	scheme = runtime.NewScheme()
	// admin is the test's own client, with every permission.
	admin client.Client
	// operatorCfg authenticates as the operator's service account.
	operatorCfg *rest.Config
	// operator is a client as the operator's service account.
	operator client.Client
)

const (
	operatorNamespace = "kvstore-operator-system"
	operatorAccount   = "controller-manager"
)

func TestMain(m *testing.M) {
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, kvv1.AddToScheme} {
		if err := add(scheme); err != nil {
			panic(err)
		}
	}
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		env.BinaryAssetsDirectory = firstDir(filepath.Join("..", "..", "bin", "k8s"))
	}
	cfg, err := env.Start()
	if err != nil {
		fmt.Fprintln(os.Stderr, "start envtest:", err)
		os.Exit(1)
	}
	code := 1
	func() {
		defer func() {
			if err := env.Stop(); err != nil {
				fmt.Fprintln(os.Stderr, "stop envtest:", err)
			}
		}()
		if admin, err = client.New(cfg, client.Options{Scheme: scheme}); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return
		}
		if err := installRole(context.Background()); err != nil {
			fmt.Fprintln(os.Stderr, "install the operator's role:", err)
			return
		}
		operatorCfg = rest.CopyConfig(cfg)
		operatorCfg.Impersonate = rest.ImpersonationConfig{UserName: "system:serviceaccount:" + operatorNamespace + ":" + operatorAccount}
		if operator, err = client.New(operatorCfg, client.Options{Scheme: scheme}); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return
		}
		code = m.Run()
	}()
	os.Exit(code)
}

// installRole creates the generated ClusterRole and binds it to the
// operator's service account, as config/rbac does.
func installRole(ctx context.Context) error {
	raw, err := os.ReadFile(filepath.Join("..", "..", "config", "rbac", "role.yaml"))
	if err != nil {
		return err
	}
	role := &rbacv1.ClusterRole{}
	if err := yaml.Unmarshal(raw, role); err != nil {
		return err
	}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: operatorNamespace}}
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: operatorAccount, Namespace: operatorNamespace}}
	binding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "manager-rolebinding"},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: role.Name},
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: operatorAccount, Namespace: operatorNamespace}},
	}
	for _, o := range []client.Object{ns, sa, role, binding} {
		if err := admin.Create(ctx, o); err != nil {
			return fmt.Errorf("create %T: %w", o, err)
		}
	}
	return nil
}

func firstDir(base string) string {
	entries, err := os.ReadDir(base)
	if err != nil {
		return ""
	}
	for _, e := range entries {
		if e.IsDir() {
			return filepath.Join(base, e.Name())
		}
	}
	return ""
}
